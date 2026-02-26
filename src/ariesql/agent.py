import json
from typing import Any, cast

from guardrails import GuardrailsAsyncOpenAI, GuardrailTripwireTriggered
from langchain.agents import create_agent  # type: ignore[unknown-arg]
from langchain.messages import AIMessage, AIMessageChunk, ToolMessage
from langchain_core.messages.utils import (
    convert_to_openai_messages,  # type: ignore[unknown-arg]
)
from langchain_core.tools import tool  # type: ignore[unknown-arg]
from langchain_daytona_data_analysis import DaytonaDataAnalysisTool
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.base import BaseCheckpointSaver

from ariesql._types import Context, DatabaseManifest
from ariesql.config import SQL_AGENT_PROMPT, format_data_scope_summary
from ariesql.context_loader import DatabaseContextLoader
from ariesql.logger import Logger
from ariesql.sql_cache import RedisSQLBank
from ariesql.sql_masker import mask_ner_and_numbers
from ariesql.tools.data_analysis_utils import process_data_analysis_result
from ariesql.tools.sql_query_tools import execute_query_mssql

logger = Logger(__name__).get_logger()

with open("src/ariesql/guardrails_config.json") as f:
    config = json.load(f)


class SQLAgent:
    MODERATION_MODEL = "omni-moderation-latest"

    def __init__(
        self,
        model: str,
        memory: BaseCheckpointSaver[str],
        context_loader: DatabaseContextLoader,
        sql_bank: RedisSQLBank,
        database_manifest: DatabaseManifest,
    ) -> None:
        self._context_loader = context_loader
        self._sql_bank = sql_bank
        self._model = model
        self._memory = memory
        self._database_manifest = database_manifest
        self._client = GuardrailsAsyncOpenAI(config=config)
        self._langchain_client = ChatOpenAI(model=self._model, root_client=self._client)
        self.data_analysis_tool = DaytonaDataAnalysisTool(
            on_result=process_data_analysis_result
        )

        @tool
        def upload_file(file_path: str, file_desc: str) -> dict[str, Any]:
            """
            Uploads a file to the data analysis tool for use in data analysis.
            Use this tool after you execute a SQL query (which should give you a file path to the results) and want to upload the results file to the data analysis tool for analysis.

            Args:
                file_path (str): The local file path to the file to be uploaded.
                file_desc (str): A description of the file being uploaded, including its contents and how it should be used in analysis.
            Returns:
                dict[str, Any]: A dictionary containing information about the uploaded file, such as its ID in the data analysis tool, the original file name, and any other relevant metadata.
            """

            logger.debug(f"Uploading file at {file_path} with description: {file_desc}")
            try:
                with open(file_path, "rb") as f:
                    sandbox_uploaded_file = self.data_analysis_tool.upload_file(  # type: ignore[unknown-arg]
                        f, file_desc
                    )
                    return sandbox_uploaded_file.model_dump()

            except Exception as e:
                logger.error(f"Error uploading file: {e}")
                return {"error": f"Error uploading file: {str(e)}"}

        self.agent = create_agent(  # type: ignore[unknown-arg]
            model=self._langchain_client,
            tools=[execute_query_mssql, self.data_analysis_tool, upload_file],
            system_prompt=SQL_AGENT_PROMPT.format(
                database_schema=self._context_loader.format_for_llm_prompt(),
                data_scope_summary=format_data_scope_summary(
                    self._database_manifest.policy
                ),
            ),
            checkpointer=self._memory,
        )

    def _construct_user_message(
        self, query: str, similar_sql_queries: list[str] | None
    ) -> str:
        user_message = f"User Query: {query}\n"
        if similar_sql_queries:
            user_message += f"Similar SQL Queries: {'\n\n'.join(similar_sql_queries)}\n"
        return user_message.strip()

    async def _input_guardrail_check(self, query: str) -> None:
        guardrail_response = await self._client.moderations.create(
            input=query, model=self.MODERATION_MODEL
        )
        if guardrail_response.results[0].flagged:
            flagged_categories: list[str] = []
            for category, flagged in (
                guardrail_response.results[0].categories.model_dump().items()
            ):
                if flagged:
                    flagged_categories.append(category)
            raise RuntimeError(
                f"Query flagged by guardrails moderation for these reasons: {flagged_categories}"
            )

    async def invoke(self, user_id: int, thread_id: str, query: str) -> str:
        await self._input_guardrail_check(query)

        logger.debug(f"User query passed guardrail check: {query}")

        is_matched_sql = False

        masked_query = mask_ner_and_numbers(query)

        similar_sql_query = await self._sql_bank.retrieve_sql(masked_query)

        if similar_sql_query:
            is_matched_sql = True

        user_message = self._construct_user_message(query, similar_sql_query)

        logger.debug(f"Invoking agent with user_message:\n{user_message}")

        try:
            res = await self.agent.ainvoke(  # type: ignore[unknown-arg]
                {
                    "messages": [
                        {
                            "role": "user",
                            "content": user_message,
                        }
                    ]
                },
                config={"configurable": {"thread_id": thread_id}},
                context=Context(
                    user_id=user_id,
                    masked_query=masked_query,
                    is_matched_sql=is_matched_sql,
                ),
            )
            return res["messages"][-1].content
        except GuardrailTripwireTriggered as e:
            raise e
        except Exception as e:
            raise RuntimeError(f"Error invoking agent: {str(e)}")
        finally:
            self.data_analysis_tool.close()

    async def stream(self, user_id: int, thread_id: str, query: str):
        await self._input_guardrail_check(query)

        logger.debug(f"User query passed guardrail check: {query}")

        is_matched_sql = False

        masked_query = mask_ner_and_numbers(query)

        similar_sql_queries = await self._sql_bank.retrieve_sql(masked_query)

        if similar_sql_queries:
            is_matched_sql = True

        user_message = self._construct_user_message(query, similar_sql_queries)

        logger.debug(f"Invoking agent with user_message:\n{user_message}")

        try:
            async for stream_mode, data in self.agent.astream(  # type: ignore[unknown-arg]
                {
                    "messages": [
                        {
                            "role": "user",
                            "content": user_message,
                        }
                    ]
                },
                config={"configurable": {"thread_id": thread_id}},
                context=Context(
                    user_id=user_id,
                    masked_query=masked_query,
                    is_matched_sql=is_matched_sql,
                ),
                stream_mode=["messages", "updates", "custom"],
            ):
                if stream_mode == "messages":
                    token, _ = data
                    if isinstance(token, AIMessageChunk):
                        if token.tool_call_chunks:
                            continue  # skip streaming tool call chunks as separate messages
                        if not str(token.content):  # type: ignore[unknown-arg]
                            continue  # skip empty tokens
                        yield cast(dict[str, Any], convert_to_openai_messages(token))
                if stream_mode == "updates":
                    for source, update in data.items():  # type: ignore[unknown-arg]
                        if source in ("model", "tools"):  # `source` captures node name
                            message = update["messages"][-1]  # type: ignore[unknown-arg]
                            if isinstance(message, AIMessage) and message.tool_calls:
                                yield cast(
                                    dict[str, Any], convert_to_openai_messages(message)
                                )
                            if isinstance(message, ToolMessage):
                                yield cast(
                                    dict[str, Any], convert_to_openai_messages(message)
                                )
                if stream_mode == "custom":
                    yield cast(dict[str, Any], data)

        except GuardrailTripwireTriggered as e:
            raise e
        except Exception as e:
            raise RuntimeError(f"Error invoking agent: {str(e)}")
        finally:
            self.data_analysis_tool.close()
