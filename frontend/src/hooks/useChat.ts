"use client";

import { useCallback, useRef, useState } from "react";
import type { ChatMessage, SSEEvent, SubEvent } from "@/types/sse";
import { SSEEventType } from "@/types/sse";

const API_URL = "/api/v1/chat/stream";

/**
 * Parse an SSE stream produced by the backend.
 * We use `fetch` + `ReadableStream` so we can POST a JSON body
 * (EventSource only supports GET).
 */
async function* streamSSE(
  query: string,
  threadId: string | null,
  signal: AbortSignal
): AsyncGenerator<SSEEvent> {
  const res = await fetch(API_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, thread_id: threadId }),
    signal,
  });

  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  if (!res.body) throw new Error("No response body");

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    // Normalise \r\n → \n so the splitter works regardless of
    // whether the SSE server uses \n or \r\n as the line separator.
    buffer = buffer.replace(/\r\n/g, "\n").replace(/\r/g, "\n");

    // SSE frames are separated by double newlines
    const parts = buffer.split("\n\n");
    buffer = parts.pop() ?? "";

    for (const part of parts) {
      let data = "";
      for (const line of part.split("\n")) {
        if (line.startsWith("data:")) {
          // SSE spec: "data:" is followed by an optional single space,
          // then the value. Do NOT .trim() — that destroys intentional
          // whitespace in the payload.
          const value = line.startsWith("data: ")
            ? line.slice(6)
            : line.slice(5);
          data += value;
        }
      }
      if (!data) continue;
      try {
        yield JSON.parse(data) as SSEEvent;
      } catch {
        /* skip malformed frames */
      }
    }
  }
}

export function useChat() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const [threadId, setThreadId] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  // ── Token buffer: accumulate content tokens and flush at ~20fps ──
  const contentBufferRef = useRef("");
  const flushTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const assistantIdRef = useRef<string>("");

  const flushContentBuffer = useCallback(() => {
    const chunk = contentBufferRef.current;
    if (!chunk) return;
    contentBufferRef.current = "";
    const aid = assistantIdRef.current;
    setMessages((prev) =>
      prev.map((m) =>
        m.id === aid ? { ...m, content: m.content + chunk } : m
      )
    );
  }, []);

  const scheduleFlush = useCallback(() => {
    if (flushTimerRef.current) return; // already scheduled
    flushTimerRef.current = setTimeout(() => {
      flushTimerRef.current = null;
      flushContentBuffer();
    }, 50); // flush every 50ms (~20fps)
  }, [flushContentBuffer]);

  const send = useCallback(
    async (query: string) => {
      if (!query.trim() || isStreaming) return;

      // Append user message
      const userMsg: ChatMessage = {
        id: crypto.randomUUID(),
        role: "user",
        content: query,
        subEvents: [],
        done: true,
      };

      // Placeholder assistant message
      const assistantId = crypto.randomUUID();
      const assistantMsg: ChatMessage = {
        id: assistantId,
        role: "assistant",
        content: "",
        subEvents: [],
        done: false,
      };

      setMessages((prev) => [...prev, userMsg, assistantMsg]);
      setIsStreaming(true);
      assistantIdRef.current = assistantId;
      contentBufferRef.current = "";

      const ctrl = new AbortController();
      abortRef.current = ctrl;

      try {
        for await (const evt of streamSSE(query, threadId, ctrl.signal)) {
          switch (evt.event) {
            case SSEEventType.CONTENT:
              // Buffer tokens and flush at ~20fps instead of per-token
              contentBufferRef.current += evt.content;
              scheduleFlush();
              break;

            case SSEEventType.TOOL_CALL:
              setMessages((prev) =>
                prev.map((m) =>
                  m.id === assistantId
                    ? {
                        ...m,
                        subEvents: [
                          ...m.subEvents,
                          ...evt.tool_calls.map(
                            (tc): SubEvent => ({
                              kind: "tool_call",
                              name: tc.function.name,
                              id: tc.id,
                              arguments: tc.function.arguments,
                            })
                          ),
                        ],
                      }
                    : m
                )
              );
              break;

            case SSEEventType.TOOL_RESULT: {
              setMessages((prev) =>
                prev.map((m) =>
                  m.id === assistantId
                    ? {
                        ...m,
                        subEvents: [
                          ...m.subEvents,
                          {
                            kind: "tool_result",
                            name: evt.name,
                            toolCallId: evt.tool_call_id,
                            content: evt.content,
                          } satisfies SubEvent,
                        ],
                      }
                    : m
                )
              );
              break;
            }

            case SSEEventType.MEDIA:
              setMessages((prev) =>
                prev.map((m) => {
                  if (m.id !== assistantId) return m;
                  // Deduplicate: skip if we already have a media event with the same src
                  const alreadyExists = m.subEvents.some(
                    (se) => se.kind === "media" && se.src === evt.content
                  );
                  if (alreadyExists) return m;
                  return {
                    ...m,
                    subEvents: [
                      ...m.subEvents,
                      { kind: "media", src: evt.content } satisfies SubEvent,
                    ],
                  };
                })
              );
              break;

            case SSEEventType.ERROR:
              setMessages((prev) =>
                prev.map((m) =>
                  m.id === assistantId
                    ? {
                        ...m,
                        subEvents: [
                          ...m.subEvents,
                          {
                            kind: "error",
                            message: evt.error,
                          } satisfies SubEvent,
                        ],
                        done: true,
                      }
                    : m
                )
              );
              break;

            case SSEEventType.DONE:
              // Flush any remaining buffered content before marking done
              if (flushTimerRef.current) {
                clearTimeout(flushTimerRef.current);
                flushTimerRef.current = null;
              }
              flushContentBuffer();
              setThreadId(evt.thread_id);
              setMessages((prev) =>
                prev.map((m) =>
                  m.id === assistantId ? { ...m, done: true } : m
                )
              );
              break;
          }
        }
      } catch (err: unknown) {
        if ((err as Error).name !== "AbortError") {
          // Flush remaining buffer before error
          if (flushTimerRef.current) {
            clearTimeout(flushTimerRef.current);
            flushTimerRef.current = null;
          }
          flushContentBuffer();
          setMessages((prev) =>
            prev.map((m) =>
              m.id === assistantId
                ? {
                    ...m,
                    subEvents: [
                      ...m.subEvents,
                      {
                        kind: "error",
                        message: String(err),
                      } satisfies SubEvent,
                    ],
                    done: true,
                  }
                : m
            )
          );
        }
      } finally {
        // Clean up any pending flush timer
        if (flushTimerRef.current) {
          clearTimeout(flushTimerRef.current);
          flushTimerRef.current = null;
        }
        setIsStreaming(false);
        abortRef.current = null;
      }
    },
    [isStreaming, threadId, scheduleFlush, flushContentBuffer]
  );

  const stop = useCallback(() => {
    abortRef.current?.abort();
  }, []);

  const reset = useCallback(() => {
    abortRef.current?.abort();
    setMessages([]);
    setThreadId(null);
    setIsStreaming(false);
  }, []);

  return { messages, isStreaming, threadId, send, stop, reset };
}
