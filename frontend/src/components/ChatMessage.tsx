"use client";

import React from "react";
import Avatar from "@mui/material/Avatar";
import SmartToyOutlinedIcon from "@mui/icons-material/SmartToyOutlined";
import PersonOutlineIcon from "@mui/icons-material/PersonOutline";
import CircularProgress from "@mui/material/CircularProgress";
import type { ChatMessage as ChatMessageType } from "@/types/sse";
import SubEventTimeline from "./SubEventTimeline";
import MarkdownRenderer from "./MarkdownRenderer";

interface Props {
  message: ChatMessageType;
}

export default function ChatMessage({ message }: Props) {
  const isUser = message.role === "user";

  /* ── User message: right-aligned bubble ─────────────────────────── */
  if (isUser) {
    return (
      <div className="flex gap-3 animate-fade-in-up justify-end">
        <div className="relative max-w-[75%] rounded-2xl px-4 py-3 bg-[#EC4899]/8 border border-[#EC4899]/15 text-[#2D3142]">
          <p className="text-sm whitespace-pre-wrap">{message.content}</p>
        </div>
        <Avatar
          sx={{
            bgcolor: "#FCE7F3",
            border: "1px solid #FBCFE8",
            width: 32,
            height: 32,
            mt: 0.5,
            flexShrink: 0,
          }}
        >
          <PersonOutlineIcon sx={{ fontSize: 18, color: "#EC4899" }} />
        </Avatar>
      </div>
    );
  }

  /* ── Assistant message: no bubble, content directly on background ── */
  return (
    <div className="flex gap-3 animate-fade-in-up justify-start">
      <Avatar
        sx={{
          background: "linear-gradient(135deg, #7C8CF8, #14B8A6)",
          width: 32,
          height: 32,
          mt: 0.5,
          flexShrink: 0,
        }}
      >
        <SmartToyOutlinedIcon sx={{ fontSize: 18, color: "#fff" }} />
      </Avatar>

      <div className="min-w-0 flex-1 text-[#2D3142]">
        {/* Sub-events (tool calls, results, media, errors) */}
        <SubEventTimeline subEvents={message.subEvents} done={!!message.done} />

        {/* Streamed markdown answer */}
        {message.content ? (
          <div className={!message.done ? "streaming-cursor" : ""}>
            <MarkdownRenderer content={message.content} />
          </div>
        ) : !message.done ? (
          <div className="flex items-center gap-2 text-xs text-[#7A7F96] py-1">
            <CircularProgress size={14} sx={{ color: "#14B8A6" }} />
            <span>Thinking…</span>
          </div>
        ) : null}
      </div>
    </div>
  );
}
