import traceback
import uuid
from typing import Any, cast

from fastapi import APIRouter, HTTPException
from sse_starlette.sse import EventSourceResponse

from ariesql.agent import SQLAgent
from ariesql.api.schemas import (
    ChatRequest,
    ContentEvent,
    DoneEvent,
    ErrorEvent,
    MediaEvent,
    SSEEvent,
    ToolCallEvent,
    ToolCallFunction,
    ToolCallItem,
    ToolResultEvent,
)
from ariesql.config import settings
from ariesql.container import get_container
from ariesql.logger import Logger

logger = Logger(__name__).get_logger()

router = APIRouter(prefix="/chat", tags=["chat"])

# Hardcoded user_id for now
USER_ID = 43624


def _serialize_sse(event: SSEEvent) -> dict[str, str]:
    """Convert a typed SSE event model into the dict that EventSourceResponse expects."""
    return {
        "event": event.event.value,
        "data": event.model_dump_json(),
    }


@router.post("/stream")
async def chat_stream(request: ChatRequest):
    """
    Stream chat responses from the SQL Agent via Server-Sent Events (SSE).

    Every SSE message has an ``event:`` field (one of the ``SSEEventType``
    values) and a JSON ``data:`` body whose shape is determined by the
    corresponding Pydantic model:

    | event         | model           | description                          |
    |---------------|-----------------|--------------------------------------|
    | ``content``   | ContentEvent    | Streamed assistant text token        |
    | ``tool_call`` | ToolCallEvent   | Agent is invoking tool(s)            |
    | ``tool_result``| ToolResultEvent| Result returned by a tool            |
    | ``media``     | MediaEvent      | Data-analysis artifact (chart, etc.) |
    | ``error``     | ErrorEvent      | Error during processing              |
    | ``done``      | DoneEvent       | Stream complete                      |
    """
    thread_id = request.thread_id or uuid.uuid4().hex

    di = get_container()
    agent = SQLAgent(
        model="gpt-5.2",
        context_loader=di.context_loader(),
        sql_bank=di.sql_bank(),
        memory=di.memory_saver(),
        database_manifest=settings.DATABASE_MANIFEST,
    )

    async def event_generator():
        try:
            async for raw_event in agent.stream(
                query=request.query,
                user_id=USER_ID,
                thread_id=thread_id,
            ):
                # convert_to_openai_messages() returns a list of dicts;
                # custom stream events (e.g. data_analysis_media) are plain dicts.
                if isinstance(raw_event, list):
                    events: list[dict[str, Any]] = raw_event  # type: ignore[assignment]
                else:
                    events = [raw_event]  # type: ignore[list-item]

                for event in events:
                    event = cast(dict[str, Any], event)  # type: ignore[unknown-arg]
                    # ── Tool call ────────────────────────────────────
                    if event.get("role") == "assistant" and event.get("tool_calls"):
                        tool_calls = [
                            ToolCallItem(
                                type=tc.get("type", "function"),
                                id=tc["id"],
                                function=ToolCallFunction(
                                    name=tc["function"]["name"],
                                    arguments=tc["function"]["arguments"],
                                ),
                            )
                            for tc in event["tool_calls"]
                        ]
                        yield _serialize_sse(
                            ToolCallEvent(thread_id=thread_id, tool_calls=tool_calls)
                        )

                    # ── Tool result ──────────────────────────────────
                    elif event.get("role") == "tool":
                        yield _serialize_sse(
                            ToolResultEvent(
                                thread_id=thread_id,
                                tool_call_id=event.get("tool_call_id", ""),
                                name=event.get("name", ""),
                                content=event.get("content", ""),
                            )
                        )

                    # ── Data analysis media (custom stream event) ────
                    elif event.get("type") == "data_analysis_media":
                        yield _serialize_sse(
                            MediaEvent(
                                thread_id=thread_id,
                                tool_name=event.get("tool_name", ""),
                                content=event.get("content", ""),
                            )
                        )

                    # ── Streamed assistant text token ────────────────
                    elif event.get("role") == "assistant" and event.get("content"):
                        yield _serialize_sse(
                            ContentEvent(
                                thread_id=thread_id,
                                content=event["content"],
                            )
                        )

        except Exception as e:
            logger.error(f"Error during chat stream: {e}")
            logger.error(traceback.format_exc())
            yield _serialize_sse(ErrorEvent(thread_id=thread_id, error=str(e)))

        # Always close with a done sentinel
        yield _serialize_sse(DoneEvent(thread_id=thread_id))

    return EventSourceResponse(event_generator())


@router.post("")
async def chat_invoke(request: ChatRequest):
    """
    Non-streaming chat endpoint. Returns the final assistant response.
    """
    thread_id = request.thread_id or uuid.uuid4().hex

    di = get_container()
    agent = SQLAgent(
        model="gpt-5.2",
        context_loader=di.context_loader(),
        sql_bank=di.sql_bank(),
        memory=di.memory_saver(),
        database_manifest=settings.DATABASE_MANIFEST,
    )

    try:
        response = await agent.invoke(
            query=request.query,
            user_id=USER_ID,
            thread_id=thread_id,
        )
        return {"thread_id": thread_id, "response": response}
    except Exception as e:
        logger.error(f"Error during chat invoke: {e}")
        raise HTTPException(status_code=500, detail=str(e))
