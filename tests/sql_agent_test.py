import json
import os
import uuid

from dotenv import load_dotenv

from ariesql.config import settings
from ariesql.container import get_container, init_container
from ariesql.logger import Logger

load_dotenv()

logger = Logger(__name__).get_logger()


async def main():
    logger.debug("Starting main, initializing DI container...")
    import timeit

    from ariesql.agent import SQLAgent

    init_container()
    di = get_container()
    logger.debug("DI container initialized successfully.")

    agent = SQLAgent(
        model="gpt-5.2",
        context_loader=di.mssql_context_loader(),
        sql_bank=di.sql_bank(),
        memory=di.memory_saver(),
        database_manifest=settings.DATABASE_MANIFEST,
    )

    user_id = 43624
    thread_id = uuid.uuid4().hex
    query = "What's my salary history and department history? Can you give an analysis on my career progression based on the data along with my title history?"

    # Deletes the old events.jsonl file and creates a new one with the streamed events from the agent
    if os.path.exists("output/events.jsonl"):
        os.remove("output/events.jsonl")

    first_event = False
    start_time = timeit.default_timer()
    with open("output/events.jsonl", "w") as f:
        async for event in agent.stream(
            query=query, user_id=user_id, thread_id=thread_id
        ):
            if not first_event:
                first_event = True
                elapsed_time = timeit.default_timer() - start_time
                logger.info(
                    f"Received first event from agent stream. Elapsed time: {elapsed_time:.2f} seconds"
                )

            if event.get("role") == "assistant" and event.get("tool_calls"):
                logger.debug(f"Received tool_calls event: {event['tool_calls']}")
            elif event.get("role") == "assistant" and event.get("content"):
                print(event["content"], end="", flush=True)

            f.write(f"{json.dumps(event)}\n")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
