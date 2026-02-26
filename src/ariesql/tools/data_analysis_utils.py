import base64
import uuid

from daytona import ExecutionArtifacts
from langgraph.config import get_stream_writer

from ariesql.logger import Logger

logger = Logger(__name__).get_logger()


def process_data_analysis_result(result: ExecutionArtifacts):
    writer = get_stream_writer()
    # logger.debug the standard output from code execution
    logger.debug(f"Result stdout: {result.stdout}")

    if not result.charts:
        logger.debug("No charts generated.")
        return

    for chart in result.charts:
        if chart.png:
            result_id = uuid.uuid4()
            # Charts are returned in base64 format
            # Decode and save them as PNG files
            path = f"media/chart-{result_id}.png"
            with open(path, "wb") as f:
                f.write(base64.b64decode(chart.png))
            writer(
                {
                    "type": "data_analysis_media",
                    "tool_name": "DataAnalysisTool",
                    "content": path,
                }
            )
            logger.debug(f"Chart saved to {path}")
