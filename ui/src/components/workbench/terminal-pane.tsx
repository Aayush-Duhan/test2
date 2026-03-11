"use client";

/**
 * Terminal component — wraps xterm.js and connects via WebSocket.
 *
 * Creates an xterm instance and hands it to `terminalStore.attachAgentTerminal()`.
 * Supports ref forwarding for style reloading and resize callbacks.
 */

import { forwardRef, memo, useEffect, useImperativeHandle, useRef } from "react";
import { FitAddon } from "@xterm/addon-fit";
import { WebLinksAddon } from "@xterm/addon-web-links";
import { Terminal as XTerm } from "@xterm/xterm";
import { terminalStore } from "@/lib/terminal-store";
import { createScopedLogger } from "@/lib/logger";
import { cn } from "@/lib/utils";

const logger = createScopedLogger('Terminal');

export interface TerminalRef {
  reloadStyles: () => void;
}

export interface TerminalPaneProps {
  className?: string;
  readonly?: boolean;
  onTerminalReady?: (terminal: XTerm) => void;
  onTerminalResize?: (cols: number, rows: number) => void;
  onTerminalDispose?: (terminal: XTerm) => void;
}

const TERMINAL_THEME = {
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
};

export const TerminalPane = memo(
  forwardRef<TerminalRef, TerminalPaneProps>(
    (
      {
        className,
        readonly = true,
        onTerminalReady,
        onTerminalResize,
        onTerminalDispose,
      },
      ref,
    ) => {
      const terminalElementRef = useRef<HTMLDivElement>(null);
      const terminalRef = useRef<XTerm | null>(null);
      const onTerminalReadyRef = useRef(onTerminalReady);
      const onTerminalResizeRef = useRef(onTerminalResize);
      const onTerminalDisposeRef = useRef(onTerminalDispose);

      useEffect(() => {
        onTerminalReadyRef.current = onTerminalReady;
        onTerminalResizeRef.current = onTerminalResize;
        onTerminalDisposeRef.current = onTerminalDispose;
      }, [onTerminalReady, onTerminalResize, onTerminalDispose]);

      useEffect(() => {
        const element = terminalElementRef.current!;
        let isMounted = true;

        const fitAddon = new FitAddon();
        const webLinksAddon = new WebLinksAddon((_event, uri) => {
          window.open(uri, '_blank');
        });

        const terminal = new XTerm({
          cursorBlink: false,
          convertEol: false,
          disableStdin: readonly,
          theme: { ...TERMINAL_THEME, cursor: readonly ? '#00000000' : TERMINAL_THEME.cursor },
          fontSize: 12,
          fontFamily:
            '"IBM Plex Mono", "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace',
          lineHeight: 1.45,
          letterSpacing: 0.2,
          scrollback: 5000,
        });

        terminalRef.current = terminal;

        terminal.loadAddon(fitAddon);
        terminal.loadAddon(webLinksAddon);
        terminal.open(element);

        // Fit after layout settles
        const fitTerminal = () => {
          if (!isMounted || terminalRef.current !== terminal) return;
          if (element.offsetWidth > 0 && element.offsetHeight > 0) {
            try {
              fitAddon.fit();
            } catch {
              logger.debug('Terminal fit skipped');
            }
            onTerminalResizeRef.current?.(terminal.cols, terminal.rows);
          }
        };
        setTimeout(fitTerminal, 0);

        const resizeObserver = new ResizeObserver(fitTerminal);
        resizeObserver.observe(element);

        logger.info('Attach terminal');

        // If a custom onTerminalReady handler is provided, use it;
        // otherwise, use the default terminalStore attachment
        if (onTerminalReadyRef.current) {
          onTerminalReadyRef.current(terminal);
        } else {
          terminalStore.attachAgentTerminal(terminal);
        }

        return () => {
          isMounted = false;
          resizeObserver.disconnect();

          if (onTerminalDisposeRef.current) {
            onTerminalDisposeRef.current(terminal);
          } else {
            terminalStore.detachAgentTerminal(terminal);
          }

          terminal.dispose();
        };
      }, [readonly]);

      useImperativeHandle(ref, () => ({
        reloadStyles: () => {
          const terminal = terminalRef.current;
          if (terminal) {
            terminal.options.theme = {
              ...TERMINAL_THEME,
              cursor: readonly ? '#00000000' : TERMINAL_THEME.cursor,
            };
          }
        },
      }), [readonly]);

      return (
        <div
          ref={terminalElementRef}
          className={cn("h-full w-full px-3 py-2", className)}
          onWheel={(e) => e.stopPropagation()}
        />
      );
    },
  ),
);

TerminalPane.displayName = 'TerminalPane';
