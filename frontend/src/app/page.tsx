"use client";

import React, { useEffect, useRef } from "react";
import IconButton from "@mui/material/IconButton";
import RestartAltRoundedIcon from "@mui/icons-material/RestartAltRounded";
import Tooltip from "@mui/material/Tooltip";
import AutoAwesomeOutlinedIcon from "@mui/icons-material/AutoAwesomeOutlined";
import { useChat } from "@/hooks/useChat";
import ChatMessage from "@/components/ChatMessage";
import ChatInput from "@/components/ChatInput";

export default function Home() {
  const { messages, isStreaming, send, stop, reset } = useChat();
  const bottomRef = useRef<HTMLDivElement>(null);

  // Auto-scroll on new content
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const hasMessages = messages.length > 0;

  return (
    <div className="flex h-dvh flex-col bg-[#FAFBFE]">
      {/* ── Header ────────────────────────────────────────────────── */}
      <header className="flex items-center justify-between border-b border-[#E8EAF0] px-6 py-3 bg-white/70 backdrop-blur-sm">
        <div className="flex items-center gap-2">
          <AutoAwesomeOutlinedIcon sx={{ color: "#F0B775", fontSize: 22 }} />
          <h1 className="text-base font-semibold tracking-tight bg-linear-to-r from-[#7C8CF8] via-[#EC4899] to-[#F0B775] bg-clip-text text-transparent">
            ArieSQL
          </h1>
          <span className="ml-2 rounded-full bg-[#10B981]/10 px-2 py-0.5 text-[10px] font-medium text-[#10B981]">
            BETA
          </span>
        </div>

        <Tooltip title="New chat" arrow>
          <IconButton onClick={reset} size="small" sx={{ color: "#7A7F96" }}>
            <RestartAltRoundedIcon sx={{ fontSize: 20 }} />
          </IconButton>
        </Tooltip>
      </header>

      {/* ── Messages area ─────────────────────────────────────────── */}
      <main className="flex-1 overflow-y-auto scrollbar-thin px-4 py-6">
        <div className="mx-auto flex max-w-3xl flex-col gap-6">
          {!hasMessages && (
            <div className="flex flex-1 flex-col items-center justify-center gap-4 py-32 animate-fade-in">
              <div className="rounded-full bg-linear-to-br from-[#7C8CF8]/15 via-[#EC4899]/10 to-[#F0B775]/15 p-4">
                <AutoAwesomeOutlinedIcon
                  sx={{ color: "#EC4899", fontSize: 36 }}
                />
              </div>
              <h2 className="text-xl font-semibold text-[#2D3142]">
                What can I help you with?
              </h2>
              <p className="max-w-md text-center text-sm text-[#7A7F96]">
                Ask questions about your data in plain English. I&apos;ll query
                the database, analyze the results, and give you a detailed
                answer.
              </p>
            </div>
          )}

          {messages.map((msg) => (
            <ChatMessage key={msg.id} message={msg} />
          ))}
          <div ref={bottomRef} />
        </div>
      </main>

      {/* ── Input bar ─────────────────────────────────────────────── */}
      <footer className="border-t border-[#E8EAF0] px-4 py-4 bg-white/50 backdrop-blur-sm">
        <div className="mx-auto max-w-3xl">
          <ChatInput onSend={send} onStop={stop} isStreaming={isStreaming} />
        </div>
      </footer>
    </div>
  );
}
