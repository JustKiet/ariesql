import { NextRequest } from "next/server";

const BACKEND_URL =
  process.env.BACKEND_URL ?? "http://localhost:8000/api/v1/chat/stream";

/**
 * Proxy the SSE stream from the FastAPI backend.
 *
 * Next.js `rewrites()` buffers the response, which kills SSE streaming.
 * A custom Route Handler with a pass-through ReadableStream avoids this.
 */
export async function POST(req: NextRequest) {
  const body = await req.text();

  const upstream = await fetch(BACKEND_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });

  if (!upstream.ok) {
    return new Response(upstream.statusText, { status: upstream.status });
  }

  // Pass the upstream SSE stream straight through without buffering.
  return new Response(upstream.body, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    },
  });
}
