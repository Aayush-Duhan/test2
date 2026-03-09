"use client";

import * as React from "react";
import { FitAddon } from "@xterm/addon-fit";
import { Terminal as XTerm } from "@xterm/xterm";
import { ChevronRight, Terminal } from "lucide-react";
import type { TerminalCommand } from "@/lib/workbench-store";

interface InlineTerminalProps {
  command: TerminalCommand;
  defaultOpen?: boolean;
}

function normalizeTerminalText(text: string): string {
  return text.replace(/\r?\n/g, "\r\n").replace(/\u0000/g, "");
}

function InlineTerminalBody({ lines }: { lines: TerminalCommand["lines"] }) {
  const hostRef = React.useRef<HTMLDivElement>(null);
  const terminalRef = React.useRef<XTerm | null>(null);
  const fitAddonRef = React.useRef<FitAddon | null>(null);

  React.useEffect(() => {
    const host = hostRef.current;
    if (!host || terminalRef.current) return;

    const terminal = new XTerm({
      allowTransparency: true,
      convertEol: false,
      cursorBlink: true,
      cursorInactiveStyle: "none",
      disableStdin: true,
      fontFamily: '"IBM Plex Mono", "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace',
      fontSize: 12,
      letterSpacing: 0.2,
      lineHeight: 1.45,
      scrollback: 5000,
      theme: {
        background: "#0a0a0a",
        foreground: "#d7dde8",
        cursor: "#f4d35e",
        cursorAccent: "#0a0a0a",
        selectionBackground: "rgba(244, 211, 94, 0.22)",
      },
    });

    const fitAddon = new FitAddon();
    terminal.loadAddon(fitAddon);
    terminal.open(host);
    fitAddon.fit();

    const observer = new ResizeObserver(() => fitAddon.fit());
    observer.observe(host);

    terminalRef.current = terminal;
    fitAddonRef.current = fitAddon;

    return () => {
      observer.disconnect();
      terminal.dispose();
      terminalRef.current = null;
      fitAddonRef.current = null;
    };
  }, []);

  React.useEffect(() => {
    const terminal = terminalRef.current;
    const fitAddon = fitAddonRef.current;
    if (!terminal || !fitAddon) return;

    terminal.reset();

    if (lines.length === 0) {
      terminal.writeln("\x1b[90mWaiting for output...\x1b[0m");
      fitAddon.fit();
      return;
    }

    let previousWasProgress = false;

    for (const line of lines) {
      const text = normalizeTerminalText(line.text);
      if (!text) continue;

      if (line.isProgress) {
        terminal.write("\r\x1b[2K");
        terminal.write(text);
        previousWasProgress = true;
        continue;
      }

      if (previousWasProgress) {
        terminal.write("\r\n");
        previousWasProgress = false;
      }

      terminal.write(text);
      if (!text.endsWith("\r") && !text.endsWith("\n")) {
        terminal.write("\r\n");
      }
    }

    fitAddon.fit();
    terminal.scrollToBottom();
  }, [lines]);

  return <div ref={hostRef} className="h-full w-full px-3 py-2" />;
}

export function InlineTerminal({ command, defaultOpen }: InlineTerminalProps) {
  const isActive = !command.isComplete;

  const [isOpen, setIsOpen] = React.useState(defaultOpen ?? isActive);

  React.useEffect(() => {
    if (isActive && command.lines.length > 0) {
      setIsOpen(true);
    }
  }, [isActive, command.lines.length]);

  const lineCount = command.lines.filter((l) => !l.isProgress).length;
  const statusDot = isActive ? "bg-emerald-400 animate-pulse" : "bg-white/25";

  return (
    <div className="flex justify-start">
      <div className="w-full max-w-[95%] overflow-hidden rounded-2xl border border-white/10 bg-[#111111]">
        <button
          type="button"
          onClick={() => setIsOpen((prev) => !prev)}
          className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-white/80 transition-colors hover:bg-white/5"
        >
          <ChevronRight className={`h-3.5 w-3.5 shrink-0 transition-transform duration-200 ${isOpen ? "rotate-90" : ""}`} />
          <Terminal className="h-3.5 w-3.5 shrink-0 text-white/50" />
          <span className="truncate font-mono text-xs font-medium">{command.label}</span>
          <span className="ml-auto flex shrink-0 items-center gap-2">
            {lineCount > 0 && <span className="text-[10px] text-white/35">{lineCount} {lineCount === 1 ? "line" : "lines"}</span>}
            <span className={`h-1.5 w-1.5 rounded-full ${statusDot}`} />
          </span>
        </button>

        {isOpen && (
          <div className="h-64 border-t border-white/10 bg-[#0a0a0a] scrollbar-dark">
            <InlineTerminalBody lines={command.lines} />
          </div>
        )}
      </div>
    </div>
  );
}
