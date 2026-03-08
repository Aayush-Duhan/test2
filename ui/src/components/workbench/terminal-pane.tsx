"use client";

import * as React from "react";
import { FitAddon } from "@xterm/addon-fit";
import { Terminal } from "@xterm/xterm";

import type { TerminalLine } from "@/lib/workbench-store";

interface TerminalPaneProps {
  lines: TerminalLine[];
}

function normalizeTerminalText(text: string): string {
  return text.replace(/\r?\n/g, "\r\n").replace(/\u0000/g, "");
}

export function TerminalPane({ lines }: TerminalPaneProps) {
  const hostRef = React.useRef<HTMLDivElement>(null);
  const terminalRef = React.useRef<Terminal | null>(null);
  const fitAddonRef = React.useRef<FitAddon | null>(null);

  React.useEffect(() => {
    const host = hostRef.current;
    if (!host || terminalRef.current) {
      return;
    }

    const terminal = new Terminal({
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

    const observer = new ResizeObserver(() => {
      fitAddon.fit();
    });
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
    if (!terminal || !fitAddon) {
      return;
    }

    terminal.reset();

    if (lines.length === 0) {
      terminal.writeln("\x1b[90mWaiting for agent commands...\x1b[0m");
      fitAddon.fit();
      return;
    }

    let previousWasProgress = false;

    for (const line of lines) {
      const text = normalizeTerminalText(line.text);
      if (!text) {
        continue;
      }

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
