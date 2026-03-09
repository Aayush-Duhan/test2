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

function sameLine(a: TerminalCommand["lines"][number], b: TerminalCommand["lines"][number]): boolean {
  return a.text === b.text && a.isProgress === b.isProgress;
}

function isPrefixMatch(prev: TerminalCommand["lines"], next: TerminalCommand["lines"], length: number): boolean {
  for (let i = 0; i < length; i++) {
    if (!sameLine(prev[i], next[i])) {
      return false;
    }
  }
  return true;
}

function InlineTerminalBody({ lines }: { lines: TerminalCommand["lines"] }) {
  const hostRef = React.useRef<HTMLDivElement>(null);
  const terminalRef = React.useRef<XTerm | null>(null);
  const fitAddonRef = React.useRef<FitAddon | null>(null);
  const renderedLinesRef = React.useRef<TerminalCommand["lines"]>([]);
  const previousWasProgressRef = React.useRef(false);

  const writeEntry = React.useCallback((terminal: XTerm, line: TerminalCommand["lines"][number]) => {
    const text = normalizeTerminalText(line.text);
    if (!text) {
      return;
    }

    if (line.isProgress) {
      terminal.write("\r\x1b[2K");
      terminal.write(text);
      previousWasProgressRef.current = true;
      return;
    }

    if (previousWasProgressRef.current) {
      terminal.write("\r\n");
      previousWasProgressRef.current = false;
    }

    terminal.write(text);
    if (!text.endsWith("\r") && !text.endsWith("\n")) {
      terminal.write("\r\n");
    }
  }, []);

  const renderFromScratch = React.useCallback((terminal: XTerm, nextLines: TerminalCommand["lines"]) => {
    terminal.reset();
    previousWasProgressRef.current = false;

    if (nextLines.length === 0) {
      terminal.writeln("\x1b[90mWaiting for output...\x1b[0m");
      renderedLinesRef.current = [];
      return;
    }

    for (const line of nextLines) {
      writeEntry(terminal, line);
    }
    renderedLinesRef.current = nextLines.slice();
  }, [writeEntry]);

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
        black: "#0a0a0a",
        red: "#ff7b72",
        green: "#7ee787",
        yellow: "#f4d35e",
        blue: "#79c0ff",
        magenta: "#d2a8ff",
        cyan: "#7ee7ff",
        white: "#d7dde8",
        brightBlack: "#6e7681",
        brightRed: "#ffa198",
        brightGreen: "#56d364",
        brightYellow: "#e3b341",
        brightBlue: "#a5d6ff",
        brightMagenta: "#e2c5ff",
        brightCyan: "#b3f0ff",
        brightWhite: "#f0f6fc",
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
    renderFromScratch(terminal, lines);
    fitAddon.fit();
    terminal.scrollToBottom();

    return () => {
      observer.disconnect();
      terminal.dispose();
      terminalRef.current = null;
      fitAddonRef.current = null;
      renderedLinesRef.current = [];
      previousWasProgressRef.current = false;
    };
  }, [lines, renderFromScratch]);

  React.useEffect(() => {
    const terminal = terminalRef.current;
    const fitAddon = fitAddonRef.current;
    if (!terminal || !fitAddon) return;

    const prevLines = renderedLinesRef.current;
    const nextLines = lines;

    if (nextLines.length === 0 || prevLines.length === 0 || nextLines.length < prevLines.length) {
      renderFromScratch(terminal, nextLines);
      fitAddon.fit();
      terminal.scrollToBottom();
      return;
    }

    const onlyLastProgressUpdated =
      nextLines.length === prevLines.length &&
      nextLines.length > 0 &&
      isPrefixMatch(prevLines, nextLines, nextLines.length - 1) &&
      prevLines[prevLines.length - 1].isProgress &&
      nextLines[nextLines.length - 1].isProgress &&
      prevLines[prevLines.length - 1].text !== nextLines[nextLines.length - 1].text;

    if (onlyLastProgressUpdated) {
      const text = normalizeTerminalText(nextLines[nextLines.length - 1].text);
      terminal.write("\r\x1b[2K");
      terminal.write(text);
      previousWasProgressRef.current = true;
      renderedLinesRef.current = nextLines.slice();
      fitAddon.fit();
      terminal.scrollToBottom();
      return;
    }

    const isAppend =
      nextLines.length >= prevLines.length &&
      isPrefixMatch(prevLines, nextLines, prevLines.length);

    if (!isAppend) {
      renderFromScratch(terminal, nextLines);
      fitAddon.fit();
      terminal.scrollToBottom();
      return;
    }

    for (let i = prevLines.length; i < nextLines.length; i++) {
      writeEntry(terminal, nextLines[i]);
    }

    renderedLinesRef.current = nextLines.slice();
    fitAddon.fit();
    terminal.scrollToBottom();
  }, [lines, renderFromScratch, writeEntry]);

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
