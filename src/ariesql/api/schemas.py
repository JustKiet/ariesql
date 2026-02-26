from enum import StrEnum

from pydantic import BaseModel, Field


# ── Request / Response ────────────────────────────────────────────────
class ChatRequest(BaseModel):
    query: str = Field(
        ..., min_length=1, description="The user's natural language query"
    )
    thread_id: str | None = Field(
        default=None,
        description="Optional conversation thread ID for multi-turn chat. "
        "A new one is generated if not provided.",
    )


class ChatResponse(BaseModel):
    thread_id: str
    response: str


# ── SSE event types ───────────────────────────────────────────────────
class SSEEventType(StrEnum):
    """Discriminator sent as the SSE ``event:`` field."""

    CONTENT = "content"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    MEDIA = "media"
    ERROR = "error"
    DONE = "done"


# ── SSE payloads (each carries its own ``event`` tag for easy switching) ─


class ContentEvent(BaseModel):
    """A single streamed text token from the assistant."""

    event: SSEEventType = SSEEventType.CONTENT
    thread_id: str
    content: str


class ToolCallFunction(BaseModel):
    name: str
    arguments: str


class ToolCallItem(BaseModel):
    type: str = "function"
    id: str
    function: ToolCallFunction


class ToolCallEvent(BaseModel):
    """The agent is invoking one or more tools."""

    event: SSEEventType = SSEEventType.TOOL_CALL
    thread_id: str
    tool_calls: list[ToolCallItem]


class ToolResultEvent(BaseModel):
    """The result returned by a tool invocation."""

    event: SSEEventType = SSEEventType.TOOL_RESULT
    thread_id: str
    tool_call_id: str
    name: str
    content: str


class MediaEvent(BaseModel):
    """A data-analysis media artifact (e.g. chart image path)."""

    event: SSEEventType = SSEEventType.MEDIA
    thread_id: str
    tool_name: str
    content: str


class ErrorEvent(BaseModel):
    """An error that occurred during streaming."""

    event: SSEEventType = SSEEventType.ERROR
    thread_id: str
    error: str


class DoneEvent(BaseModel):
    """Signals that the stream has completed."""

    event: SSEEventType = SSEEventType.DONE
    thread_id: str


# Union of all possible SSE payloads – handy for client-side typing.
SSEEvent = (
    ContentEvent | ToolCallEvent | ToolResultEvent | MediaEvent | ErrorEvent | DoneEvent
)
