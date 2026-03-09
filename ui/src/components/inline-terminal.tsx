"use client";

import * as React from "react";
import { ChevronRight, Terminal } from "lucide-react";
import type { TerminalCommand } from "@/lib/workbench-store";

interface InlineTerminalProps {
  command: TerminalCommand;
  defaultOpen?: boolean;
}

export function InlineTerminal({ command, defaultOpen }: InlineTerminalProps) {
  const isActive = !command.isComplete;

  // Active commands start open, completed ones start collapsed
  const [isOpen, setIsOpen] = React.useState(defaultOpen ?? isActive);

  // Auto-expand when a previously empty command receives its first line
  React.useEffect(() => {
    if (isActive && command.lines.length > 0) {
      setIsOpen(true);
    }
  }, [isActive, command.lines.length]);

  const lineCount = command.lines.filter((l) => !l.isProgress).length;
  const statusDot = isActive
    ? "bg-emerald-400 animate-pulse"
    : "bg-white/25";

  return (
    <div className="flex justify-start">
      <div className="w-full max-w-[95%] rounded-2xl border border-white/10 bg-[#111111] overflow-hidden">
        {/* Header — clickable toggle */}
        <button
          type="button"
          onClick={() => setIsOpen((prev) => !prev)}
          className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-white/80 transition-colors hover:bg-white/5"
        >
          <ChevronRight
            className={`h-3.5 w-3.5 shrink-0 transition-transform duration-200 ${isOpen ? "rotate-90" : ""}`}
          />
          <Terminal className="h-3.5 w-3.5 shrink-0 text-white/50" />
          <span className="font-medium font-mono text-xs truncate">
            {command.label}
          </span>
          <span className="ml-auto flex items-center gap-2 shrink-0">
            {lineCount > 0 && (
              <span className="text-[10px] text-white/35">
                {lineCount} {lineCount === 1 ? "line" : "lines"}
              </span>
            )}
            <span className={`h-1.5 w-1.5 rounded-full ${statusDot}`} />
          </span>
        </button>

        {/* Terminal output body */}
        {isOpen && (
          <div className="border-t border-white/10 bg-[#0a0a0a] max-h-64 overflow-y-auto scrollbar-dark">
            <pre className="px-3 py-2 text-xs leading-relaxed font-mono text-[#d7dde8] whitespace-pre-wrap break-words">
              {command.lines.length === 0 ? (
                <span className="text-white/25">Waiting for output...</span>
              ) : (
                command.lines.map((line, i) => {
                  if (line.isProgress) {
                    // Only show the latest progress line (last one that is progress)
                    const isLastProgress =
                      i === command.lines.length - 1 ||
                      !command.lines.slice(i + 1).some((l) => l.isProgress);
                    if (!isLastProgress) return null;
                    return (
                      <span key={i} className="text-yellow-300/70">
                        {line.text}
                        {"\n"}
                      </span>
                    );
                  }
                  return (
                    <React.Fragment key={i}>
                      {line.text}
                      {i < command.lines.length - 1 ? "\n" : ""}
                    </React.Fragment>
                  );
                })
              )}
            </pre>
            {/* Scroll anchor — overflow-anchor pins scroll to bottom when user is at bottom */}
            <div style={{ overflowAnchor: "auto", height: 1 }} />
          </div>
        )}
      </div>
    </div>
  );
}
