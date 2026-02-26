"use client";

import React, { useRef, useState } from "react";
import IconButton from "@mui/material/IconButton";
import SendRoundedIcon from "@mui/icons-material/SendRounded";
import StopCircleOutlinedIcon from "@mui/icons-material/StopCircleOutlined";

interface Props {
  onSend: (query: string) => void;
  onStop: () => void;
  isStreaming: boolean;
}

export default function ChatInput({ onSend, onStop, isStreaming }: Props) {
  const [value, setValue] = useState("");
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const submit = () => {
    const trimmed = value.trim();
    if (!trimmed || isStreaming) return;
    onSend(trimmed);
    setValue("");
    if (textareaRef.current) textareaRef.current.style.height = "auto";
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      submit();
    }
  };

  const autoGrow = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setValue(e.target.value);
    const el = e.target;
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, 200)}px`;
  };

  return (
    <div className="flex items-center gap-2 rounded-2xl border border-[#E0E3ED] bg-white px-4 py-3 shadow-sm transition-all focus-within:border-[#14B8A6]/50 focus-within:shadow-[#14B8A6]/10 focus-within:shadow-md">
      <textarea
        ref={textareaRef}
        value={value}
        onChange={autoGrow}
        onKeyDown={handleKeyDown}
        placeholder="Ask anything about your data…"
        rows={1}
        className="flex-1 resize-none bg-transparent text-sm text-[#2D3142] placeholder-[#B0B5C8] outline-none scrollbar-thin py-1"
      />

      {isStreaming ? (
        <IconButton onClick={onStop} size="small" sx={{ color: "#EF4444" }}>
          <StopCircleOutlinedIcon />
        </IconButton>
      ) : (
        <IconButton
          onClick={submit}
          size="small"
          disabled={!value.trim()}
          sx={{
            color: value.trim() ? "#14B8A6" : "#C8CCD8",
            transition: "color 0.2s",
          }}
        >
          <SendRoundedIcon />
        </IconButton>
      )}
    </div>
  );
}
