/* ── SSE event types mirroring backend schemas.py ──────────────────── */

export enum SSEEventType {
  CONTENT = "content",
  TOOL_CALL = "tool_call",
  TOOL_RESULT = "tool_result",
  MEDIA = "media",
  ERROR = "error",
  DONE = "done",
}

/* ── Payload shapes ────────────────────────────────────────────────── */

export interface ContentEvent {
  event: SSEEventType.CONTENT;
  thread_id: string;
  content: string;
}

export interface ToolCallFunction {
  name: string;
  arguments: string;
}

export interface ToolCallItem {
  type: string;
  id: string;
  function: ToolCallFunction;
}

export interface ToolCallEvent {
  event: SSEEventType.TOOL_CALL;
  thread_id: string;
  tool_calls: ToolCallItem[];
}

export interface ToolResultEvent {
  event: SSEEventType.TOOL_RESULT;
  thread_id: string;
  tool_call_id: string;
  name: string;
  content: string;
}

export interface MediaEvent {
  event: SSEEventType.MEDIA;
  thread_id: string;
  tool_name: string;
  content: string;
}

export interface ErrorEvent {
  event: SSEEventType.ERROR;
  thread_id: string;
  error: string;
}

export interface DoneEvent {
  event: SSEEventType.DONE;
  thread_id: string;
}

export type SSEEvent =
  | ContentEvent
  | ToolCallEvent
  | ToolResultEvent
  | MediaEvent
  | ErrorEvent
  | DoneEvent;

/* ── Local chat types ──────────────────────────────────────────────── */

export type SubEvent =
  | { kind: "tool_call"; name: string; id: string; arguments: string }
  | {
      kind: "tool_result";
      name: string;
      toolCallId: string;
      content: string;
    }
  | { kind: "media"; src: string }
  | { kind: "error"; message: string };

export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string; // accumulated markdown for assistant
  subEvents: SubEvent[];
  done: boolean;
}
