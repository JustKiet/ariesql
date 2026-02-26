"use client";

import React, { useState, useMemo, useEffect, useRef } from "react";
import StorageOutlinedIcon from "@mui/icons-material/StorageOutlined";
import CodeOutlinedIcon from "@mui/icons-material/CodeOutlined";
import UploadFileOutlinedIcon from "@mui/icons-material/UploadFileOutlined";
import BuildCircleOutlinedIcon from "@mui/icons-material/BuildCircleOutlined";
import CheckCircleOutlineIcon from "@mui/icons-material/CheckCircleOutline";
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";
import InsertPhotoOutlinedIcon from "@mui/icons-material/InsertPhotoOutlined";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import Collapse from "@mui/material/Collapse";
import CircularProgress from "@mui/material/CircularProgress";
import type { SubEvent } from "@/types/sse";

/* ── helpers ─────────────────────────────────────────────────────────── */

function humanize(name: string) {
  return name
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function tryPrettyJson(raw: string): string {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

function extractSqlQuery(argsJson: string): string | null {
  try {
    return JSON.parse(argsJson).query ?? null;
  } catch {
    return null;
  }
}

/** Extract python code from daytona_data_analysis arguments. */
function extractPythonCode(argsJson: string): string | null {
  try {
    return JSON.parse(argsJson).data_analysis_python_code ?? null;
  } catch {
    return null;
  }
}

/** Extract upload_file arguments. */
function extractUploadArgs(argsJson: string): {
  filePath: string;
  fileDesc: string;
} | null {
  try {
    const p = JSON.parse(argsJson);
    return { filePath: p.file_path ?? "", fileDesc: p.file_desc ?? "" };
  } catch {
    return null;
  }
}

/** Parse upload_file result content. */
function parseUploadResult(raw: string): {
  name: string;
  remotePath: string;
  description: string;
} | null {
  try {
    const p = JSON.parse(raw);
    return {
      name: p.name ?? "",
      remotePath: p.remote_path ?? "",
      description: p.description ?? "",
    };
  } catch {
    return null;
  }
}

/** Parse tool result content for SQL tools. */
function parseToolResultContent(raw: string): {
  results: Record<string, string>[] | null;
  error: string | null;
  filePath: string | null;
} {
  try {
    const parsed = JSON.parse(raw);
    return {
      results: parsed.results ?? null,
      error: parsed.error ?? null,
      filePath: parsed.file_path ?? null,
    };
  } catch {
    return { results: null, error: null, filePath: null };
  }
}

/* ── Tiny icon for each tool type ────────────────────────────────────── */

function ToolIcon({ name }: { name: string }) {
  const sx = { fontSize: 14 };
  if (name === "execute_query_mssql")
    return <StorageOutlinedIcon sx={sx} className="text-[#7C8CF8]" />;
  if (name === "daytona_data_analysis")
    return <CodeOutlinedIcon sx={sx} className="text-[#6BC9A0]" />;
  if (name === "upload_file")
    return <UploadFileOutlinedIcon sx={sx} className="text-[#F0B775]" />;
  return <BuildCircleOutlinedIcon sx={sx} className="text-[#7C8CF8]" />;
}

/* ── ChatGPT-style code block ────────────────────────────────────────── */

function CodeBlock({
  code,
  language,
}: {
  code: string;
  language: string;
}) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="my-2 rounded-lg overflow-hidden border border-[#E0E3ED] code-block-wrapper">
      {/* Header bar */}
      <div className="flex items-center justify-between px-4 py-2 bg-[#F0F2F8] border-b border-[#E0E3ED]">
        <span className="text-[11px] font-medium text-[#7A7F96] lowercase">
          {language}
        </span>
        <button
          onClick={handleCopy}
          className="flex items-center gap-1 text-[11px] text-[#7A7F96] hover:text-[#2D3142] transition-colors cursor-pointer"
        >
          <ContentCopyIcon sx={{ fontSize: 13 }} />
          {copied ? "Copied!" : "Copy code"}
        </button>
      </div>
      {/* Code body */}
      <pre className="p-4 overflow-x-auto bg-[#F8F9FC] m-0">
        <code className="text-[12px] leading-relaxed font-mono text-[#2D3142] whitespace-pre">
          {code}
        </code>
      </pre>
    </div>
  );
}

/* ── Collapsible detail section ──────────────────────────────────────── */

function DetailBlock({
  label,
  children,
  defaultOpen = false,
}: {
  label: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="mt-1">
      <button
        onClick={() => setOpen((p) => !p)}
        className="flex items-center gap-1 text-[11px] text-[#14B8A6] hover:text-[#0D9488] transition-colors cursor-pointer select-none"
      >
        {open ? (
          <ExpandLessIcon sx={{ fontSize: 13 }} />
        ) : (
          <ExpandMoreIcon sx={{ fontSize: 13 }} />
        )}
        {label}
      </button>
      <Collapse in={open} timeout={200}>
        <div className="mt-1">{children}</div>
      </Collapse>
    </div>
  );
}

/* ── Result table ────────────────────────────────────────────────────── */

function ResultTable({ rows }: { rows: Record<string, string>[] }) {
  if (rows.length === 0)
    return (
      <span className="text-[11px] text-[#9494A3] italic">
        Query returned 0 rows
      </span>
    );

  const cols = Object.keys(rows[0]);
  const displayRows = rows.slice(0, 20);
  const truncated = rows.length > 20;

  return (
    <div className="overflow-x-auto rounded-lg border border-[#E0E3ED]">
      <table className="w-full text-[11px] text-[#2D3142]">
        <thead>
          <tr className="bg-[#F0F2F8]">
            {cols.map((col) => (
              <th
                key={col}
                className="px-3 py-1.5 text-left font-medium text-[#7A7F96] whitespace-nowrap"
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayRows.map((row, i) => (
            <tr
              key={i}
              className="border-t border-[#E0E3ED] hover:bg-[#F5F6FA]"
            >
              {cols.map((col) => (
                <td key={col} className="px-3 py-1.5 whitespace-nowrap">
                  {row[col]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {truncated && (
        <div className="px-3 py-1.5 text-[10px] text-[#7A7F96] bg-[#F0F2F8] border-t border-[#E0E3ED]">
          Showing first 20 of {rows.length} rows
        </div>
      )}
    </div>
  );
}

/* ── Pastel pill color palette — keyed by tool name ──────────────────── */

const TOOL_PILL_MAP: Record<string, { bg: string; text: string; dot: string }> = {
  execute_query_mssql:    { bg: "bg-[#DBEAFE]", text: "text-[#3B82F6]", dot: "bg-[#3B82F6]" },    // Sky blue
  daytona_data_analysis:  { bg: "bg-[#D1FAE5]", text: "text-[#10B981]", dot: "bg-[#10B981]" },    // Mint green
  upload_file:            { bg: "bg-[#FEF3C7]", text: "text-[#D97706]", dot: "bg-[#D97706]" },    // Amber
};

const FALLBACK_PILLS = [
  { bg: "bg-[#EDE9FE]", text: "text-[#7C3AED]", dot: "bg-[#7C3AED]" },    // Lavender
  { bg: "bg-[#FCE7F3]", text: "text-[#EC4899]", dot: "bg-[#EC4899]" },    // Pink
  { bg: "bg-[#CCFBF1]", text: "text-[#14B8A6]", dot: "bg-[#14B8A6]" },    // Teal
  { bg: "bg-[#FFEDD5]", text: "text-[#EA580C]", dot: "bg-[#EA580C]" },    // Orange
  { bg: "bg-[#FEE2E2]", text: "text-[#EF4444]", dot: "bg-[#EF4444]" },    // Coral
];

/** Dynamically assigned colors for tools not in the static map. */
const dynamicToolColors = new Map<string, { bg: string; text: string; dot: string }>();
let fallbackIdx = 0;

function getPillForTool(name: string): { bg: string; text: string; dot: string } {
  if (TOOL_PILL_MAP[name]) return TOOL_PILL_MAP[name];
  if (!dynamicToolColors.has(name)) {
    dynamicToolColors.set(name, FALLBACK_PILLS[fallbackIdx % FALLBACK_PILLS.length]);
    fallbackIdx++;
  }
  return dynamicToolColors.get(name)!;
}

/* ── Main component ──────────────────────────────────────────────────── */

interface Props {
  subEvents: SubEvent[];
  done?: boolean;
}

export default function SubEventTimeline({ subEvents, done }: Props) {
  const [expanded, setExpanded] = useState(true);
  const wasDone = useRef(false);

  // Auto-collapse the timeline once streaming finishes
  useEffect(() => {
    if (done && !wasDone.current) {
      wasDone.current = true;
      // Small delay so the user can see the final step complete
      const t = setTimeout(() => setExpanded(false), 600);
      return () => clearTimeout(t);
    }
  }, [done]);

  // Build a map of toolCallId → tool_result for pairing
  const resultMap = useMemo(() => {
    const m = new Map<string, { name: string; content: string }>();
    for (const se of subEvents) {
      if (se.kind === "tool_result") {
        m.set(se.toolCallId, { name: se.name, content: se.content });
      }
    }
    return m;
  }, [subEvents]);

  if (subEvents.length === 0) return null;

  // Count tool calls only (for the step counter in the header)
  const toolCalls = subEvents.filter((se) => se.kind === "tool_call");
  const completedCalls = toolCalls.filter(
    (se) => se.kind === "tool_call" && resultMap.has(se.id)
  );

  // Separate media events (always visible) from step events
  const mediaEvents = subEvents.filter((se) => se.kind === "media");
  // Steps: tool_calls + errors (skip tool_results, they render under their call)
  const stepEvents = subEvents.filter(
    (se) => se.kind !== "tool_result" && se.kind !== "media"
  );

  return (
    <div className="mb-4">
      {/* ── Perplexity-style toggle header ── */}
      {stepEvents.length > 0 && (
        <>
          <button
            onClick={() => setExpanded((p) => !p)}
            className="group flex items-center gap-2 text-xs text-[#7A7F96] hover:text-[#2D3142] transition-colors cursor-pointer select-none py-1"
          >
            {expanded ? (
              <ExpandLessIcon sx={{ fontSize: 16 }} />
            ) : (
              <ExpandMoreIcon sx={{ fontSize: 16 }} />
            )}
            <span className="font-medium">
              {completedCalls.length < toolCalls.length ? (
                <>
                  <CircularProgress
                    size={10}
                    sx={{ color: "#14B8A6", mr: 0.5 }}
                  />
                  {" "}Working…
                </>
              ) : (
                <>
                  Analyzed {toolCalls.length} step
                  {toolCalls.length !== 1 ? "s" : ""}
                </>
              )}
            </span>
            {/* Mini pill strip when collapsed */}
            {!expanded && (
              <span className="flex items-center gap-1.5 ml-1">
                {toolCalls.slice(0, 5).map((tc, idx) => {
                  const pill = getPillForTool(tc.kind === "tool_call" ? tc.name : "");
                  return (
                    <span
                      key={idx}
                      className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium ${pill.bg} ${pill.text}`}
                    >
                      <ToolIcon name={tc.kind === "tool_call" ? tc.name : ""} />
                    </span>
                  );
                })}
                {toolCalls.length > 5 && (
                  <span className="text-[10px]">+{toolCalls.length - 5}</span>
                )}
              </span>
            )}
          </button>

          <Collapse in={expanded} timeout={300}>
            {/* ── Flat timeline (Perplexity-style) ── */}
            <div className="ml-1 mt-1 border-l border-[#E0E3ED] pl-4 space-y-3">
              {stepEvents.map((se, i) => {
                /* ── tool_call ── */
                if (se.kind === "tool_call") {
                  const result = resultMap.get(se.id);
                  const isSQL = se.name === "execute_query_mssql";
                  const isDaytona = se.name === "daytona_data_analysis";
                  const isUpload = se.name === "upload_file";
                  const pill = getPillForTool(se.name);

                  return (
                    <div
                      key={`${se.id}-${i}`}
                      className="animate-fade-in-up"
                      style={{ animationDelay: `${i * 30}ms` }}
                    >
                      {/* ── Title pill ── */}
                      <div className="flex items-center gap-2">
                        {/* Timeline dot */}
                        <span className="-ml-[21px] flex items-center justify-center w-[10px] h-[10px] rounded-full bg-[#FAFBFE] border border-[#E0E3ED]">
                          <span
                            className={`w-[5px] h-[5px] rounded-full ${
                              result ? "bg-[#10B981]" : `${pill.dot} animate-pulse`
                            }`}
                          />
                        </span>
                        <span
                          className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-[11px] font-semibold ${pill.bg}`}
                        >
                          <ToolIcon name={se.name} />
                          <span className={pill.text}>
                            {humanize(se.name)}
                          </span>
                          {result ? (
                            <CheckCircleOutlineIcon
                              sx={{ fontSize: 12 }}
                              className="text-[#10B981]"
                            />
                          ) : (
                            <CircularProgress size={10} sx={{ color: "#9CA3AF" }} />
                          )}
                        </span>
                      </div>

                      {/* ── SQL tool ── */}
                      {isSQL && (() => {
                        const sqlQuery = extractSqlQuery(se.arguments);
                        const parsed = result
                          ? parseToolResultContent(result.content)
                          : null;
                        return (
                          <div className="ml-6 mt-1 space-y-1">
                            {sqlQuery && (
                              <DetailBlock label="SQL query">
                                <CodeBlock code={sqlQuery} language="sql" />
                              </DetailBlock>
                            )}
                            {parsed?.error && (
                              <div className="flex items-start gap-2 rounded-lg bg-[#FEE2E2] border border-[#FECACA] px-3 py-2 mt-1">
                                <ErrorOutlineIcon
                                  sx={{ fontSize: 13, mt: "1px" }}
                                  className="text-[#EF4444] shrink-0"
                                />
                                <span className="text-[11px] text-[#DC2626] break-all">
                                  {parsed.error}
                                </span>
                              </div>
                            )}
                            {parsed?.results && parsed.results.length > 0 && (
                              <DetailBlock
                                label={`Results (${parsed.results.length} row${parsed.results.length !== 1 ? "s" : ""})`}
                              >
                                <ResultTable rows={parsed.results} />
                              </DetailBlock>
                            )}
                            {parsed?.results &&
                              parsed.results.length === 0 &&
                              !parsed.error && (
                                <span className="text-[11px] text-[#7A7F96] italic ml-1">
                                  0 rows returned
                                </span>
                              )}
                          </div>
                        );
                      })()}

                      {/* ── Daytona (Python code) ── */}
                      {isDaytona && (() => {
                        const code = extractPythonCode(se.arguments);
                        return (
                          <div className="ml-6 mt-1 space-y-1">
                            {code && (
                              <DetailBlock label="Python code" defaultOpen={false}>
                                <CodeBlock code={code} language="python" />
                              </DetailBlock>
                            )}
                            {result && (
                              <DetailBlock label="Execution output">
                                <pre className="rounded-lg bg-[#F8F9FC] border border-[#E0E3ED] p-3 text-[11px] text-[#7A7F96] overflow-x-auto whitespace-pre-wrap font-mono leading-relaxed max-h-40 overflow-y-auto">
                                  {result.content}
                                </pre>
                              </DetailBlock>
                            )}
                          </div>
                        );
                      })()}

                      {/* ── Upload file ── */}
                      {isUpload && (() => {
                        const args = extractUploadArgs(se.arguments);
                        const uploadResult = result
                          ? parseUploadResult(result.content)
                          : null;
                        return (
                          <div className="ml-6 mt-1.5">
                            {args && (
                              <div className="flex items-start gap-2 text-[11px]">
                                <span className="text-[#7A7F96] shrink-0">
                                  File:
                                </span>
                                <span className="text-[#2D3142]/80 font-mono text-[11px]">
                                  {args.filePath}
                                </span>
                              </div>
                            )}
                            {args?.fileDesc && (
                              <div className="flex items-start gap-2 text-[11px] mt-0.5">
                                <span className="text-[#7A7F96] shrink-0">
                                  Desc:
                                </span>
                                <span className="text-[#2D3142]/60 text-[11px]">
                                  {args.fileDesc}
                                </span>
                              </div>
                            )}
                            {uploadResult && (
                              <div className="flex items-center gap-1.5 mt-1 text-[11px] text-[#10B981]">
                                <CheckCircleOutlineIcon sx={{ fontSize: 12 }} />
                                <span>
                                  Uploaded to{" "}
                                  <span className="font-mono text-[10px] text-[#7A7F96]">
                                    {uploadResult.remotePath}
                                  </span>
                                </span>
                              </div>
                            )}
                          </div>
                        );
                      })()}

                      {/* ── Generic fallback for other tools ── */}
                      {!isSQL && !isDaytona && !isUpload && (
                        <div className="ml-6 mt-1 space-y-1">
                          {se.arguments && (
                            <DetailBlock label="Arguments">
                              <pre className="rounded-lg bg-[#F8F9FC] border border-[#E0E3ED] p-3 text-[11px] text-[#2D3142]/70 overflow-x-auto whitespace-pre-wrap font-mono leading-relaxed">
                                {tryPrettyJson(se.arguments)}
                              </pre>
                            </DetailBlock>
                          )}
                          {result && (
                            <DetailBlock label="Result">
                              <pre className="rounded-lg bg-[#F8F9FC] border border-[#E0E3ED] p-3 text-[11px] text-[#2D3142]/70 overflow-x-auto whitespace-pre-wrap font-mono leading-relaxed max-h-40 overflow-y-auto">
                                {tryPrettyJson(result.content)}
                              </pre>
                            </DetailBlock>
                          )}
                        </div>
                      )}
                    </div>
                  );
                }

                /* ── error ── */
                if (se.kind === "error") {
                  return (
                    <div
                      key={`error-${i}`}
                      className="animate-fade-in-up"
                      style={{ animationDelay: `${i * 30}ms` }}
                    >
                      <div className="flex items-center gap-2">
                        <span className="-ml-[21px] flex items-center justify-center w-[10px] h-[10px] rounded-full bg-[#FAFBFE] border border-[#FECACA]">
                          <span className="w-[5px] h-[5px] rounded-full bg-[#F87171]" />
                        </span>
                        <span className="inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-[11px] font-medium bg-[#FEE2E2]">
                          <ErrorOutlineIcon
                            sx={{ fontSize: 13 }}
                            className="text-[#EF4444]"
                          />
                          <span className="text-[#DC2626] break-all">
                            {se.message}
                          </span>
                        </span>
                      </div>
                    </div>
                  );
                }

                return null;
              })}
            </div>
          </Collapse>
        </>
      )}

      {/* ── Media images — below steps, above the answer ── */}
      {mediaEvents.map((se, i) =>
        se.kind === "media" ? (
          <div
            key={`media-always-${i}`}
            className="mt-3 mb-1 animate-fade-in-up"
          >
            <div className="flex items-center gap-2 mb-2">
              <InsertPhotoOutlinedIcon
                sx={{ fontSize: 14 }}
                className="text-[#F0B775]"
              />
              <span className="text-[12px] text-[#7A7F96] font-medium">
                Generated Chart
              </span>
            </div>
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src={`/${se.src}`}
              alt="Data analysis chart"
              className="w-full max-w-2xl rounded-xl border border-[#E0E3ED] shadow-sm"
            />
          </div>
        ) : null
      )}
    </div>
  );
}
