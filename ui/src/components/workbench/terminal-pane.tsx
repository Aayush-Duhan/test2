"use client";

import { useEffect, useRef } from "react";
import { FitAddon } from "@xterm/addon-fit";
import { Terminal as XTerm } from "@xterm/xterm";
import { terminalStore } from "@/lib/terminal-store";

/**
 * Workbench terminal pane — exact copy of bolt.new's Terminal.tsx pattern.
 *
 * Creates an xterm instance and hands it to `terminalStore.attachAgentTerminal()`.
 * The store opens a WebSocket and does:
 *   ws.onmessage = (event) => terminal.write(event.data)
 *
 * Zero React state. Zero SSE. Raw PTY bytes straight to xterm.
 */
export function TerminalPane() {
  const terminalElementRef = useRef<HTMLDivElement>(null);
  const terminalRef = useRef<XTerm | null>(null);

  useEffect(() => {
    const element = terminalElementRef.current!;
    let isMounted = true;

    const fitAddon = new FitAddon();

    const terminal = new XTerm({
      cursorBlink: false,
      convertEol: false,
      disableStdin: true,
      theme: {
        background: "#0a0a0a",
        foreground: "#d7dde8",
        cursor: "#00000000",
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
      fontSize: 12,
      fontFamily: '"IBM Plex Mono", "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace',
      lineHeight: 1.45,
      letterSpacing: 0.2,
      scrollback: 5000,
    });

    terminalRef.current = terminal;
    terminal.loadAddon(fitAddon);
    terminal.open(element);

    // Fit after layout settles (same as bolt.new)
    const fitTerminal = () => {
      if (!isMounted || terminalRef.current !== terminal) return;
      if (element.offsetWidth > 0 && element.offsetHeight > 0) {
        try {
          fitAddon.fit();
        } catch {
          /* skip */
        }
        // Notify store of terminal resize (for WebSocket resize message)
        terminalStore.onAgentTerminalResize(terminal.cols, terminal.rows);
      }
    };
    setTimeout(fitTerminal, 0);

    const resizeObserver = new ResizeObserver(fitTerminal);
    resizeObserver.observe(element);

    // Attach via WebSocket — bolt.new's pattern:
    // ws.onmessage = (event) => terminal.write(event.data)
    terminalStore.attachAgentTerminal(terminal);

    return () => {
      isMounted = false;
      resizeObserver.disconnect();
      terminalStore.detachAgentTerminal(terminal);
      terminal.dispose();
    };
  }, []);

  return (
    <div
      ref={terminalElementRef}
      className="h-full w-full px-3 py-2"
      onWheel={(e) => e.stopPropagation()}
    />
  );
}
