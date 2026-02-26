"use client";

import React, { useMemo } from "react";
import { marked } from "marked";
import DOMPurify from "dompurify";

interface Props {
  content: string;
}

// Configure marked once
marked.setOptions({
  gfm: true,
  breaks: true,  // Single \n → <br> — important for streaming LLM output
});

/**
 * Minimal pre-processing for streamed markdown.
 *
 * The backend sends well-formed markdown. The ONLY issue is that during
 * streaming, a bold/italic marker may be open but not yet closed
 * (the closing token hasn't arrived). We strip the dangling marker
 * so `marked` doesn't break the entire paragraph.
 */
function preprocessMarkdown(raw: string): string {
  return stripDanglingMarkers(raw);
}

/**
 * Strip unclosed ** or * markers at the end of streamed content.
 * Instead of trying to close them (which creates artifacts),
 * we remove from the last unclosed marker to the end.
 */
function stripDanglingMarkers(md: string): string {
  if (md.length < 2) return md;

  // Ignore markers inside code spans
  const withoutCode = md.replace(/`[^`]*`/g, (m) => " ".repeat(m.length));

  // Handle ** (bold)
  const boldPositions: number[] = [];
  let searchFrom = 0;
  while (true) {
    const idx = withoutCode.indexOf("**", searchFrom);
    if (idx === -1) break;
    boldPositions.push(idx);
    searchFrom = idx + 2;
  }

  if (boldPositions.length % 2 !== 0 && boldPositions.length > 0) {
    const lastBoldPos = boldPositions[boldPositions.length - 1];
    md = md.slice(0, lastBoldPos).replace(/\s+$/, "");
    return stripDanglingMarkers(md);
  }

  // Handle * (italic) — excluding ** pairs
  const withoutBoldPairs = withoutCode.replace(/\*\*/g, "  ");
  const italicPositions: number[] = [];
  for (let i = 0; i < withoutBoldPairs.length; i++) {
    if (withoutBoldPairs[i] === "*") {
      italicPositions.push(i);
    }
  }

  if (italicPositions.length % 2 !== 0 && italicPositions.length > 0) {
    const lastItalicPos = italicPositions[italicPositions.length - 1];
    md = md.slice(0, lastItalicPos).replace(/\s+$/, "");
  }

  return md;
}

export default function MarkdownRenderer({ content }: Props) {
  const html = useMemo(() => {
    const processed = preprocessMarkdown(content);
    const rawHtml = marked.parse(processed, { async: false }) as string;
    return DOMPurify.sanitize(rawHtml, {
      ADD_TAGS: ["img"],
      ADD_ATTR: ["src", "alt", "class"],
    });
  }, [content]);

  return (
    <div
      className="markdown-body"
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}
