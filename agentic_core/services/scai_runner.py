"""CLI execution helpers for SCAI commands."""

from __future__ import annotations

import logging
import time
from typing import Callable, List, Optional

from agentic_core.utils.text import strip_ansi

logger = logging.getLogger(__name__)

TerminalCallback = Callable[[str, bool], None]


def _emit_terminal_output(
    terminal_callback: Optional[TerminalCallback],
    text: str,
    is_progress: bool,
) -> None:
    """Forward raw terminal text to the dedicated terminal stream."""
    if terminal_callback is None:
        return

    payload = text.replace("\x00", "")
    if not payload:
        return

    try:
        terminal_callback(payload, is_progress)
    except Exception:
        pass


def run_scai_command_pty(
    cmd: List[str],
    cwd: str,
    line_callback: Optional[Callable[[str, bool], None]] = None,
    terminal_callback: Optional[TerminalCallback] = None,
) -> tuple[int, str, str]:
    """Run a CLI command inside a PTY so progress bars and CR overwrites work."""
    from winpty import PtyProcess  # type: ignore[import-untyped]

    argv = " ".join(cmd)
    proc = PtyProcess.spawn(f"cmd /c cd /d {cwd} && {argv}", dimensions=(24, 200))

    raw_buf = ""
    stdout_lines: List[str] = []
    last_cr_line = ""

    while proc.isalive():
        try:
            chunk = proc.read(4096)
        except EOFError:
            break
        except Exception:
            time.sleep(0.02)
            continue

        raw_buf += chunk

        # Broadcast raw chunk to all WebSocket terminal clients (bolt.new pattern)
        try:
            from python_execution_service import terminal_bridge
            terminal_bridge.broadcast(chunk)
        except Exception:
            pass

        while "\n" in raw_buf or "\r" in raw_buf:
            cr_idx = raw_buf.find("\r")
            nl_idx = raw_buf.find("\n")

            if nl_idx != -1 and (cr_idx == -1 or nl_idx < cr_idx):
                line_text = raw_buf[:nl_idx]
                raw_buf = raw_buf[nl_idx + 1 :]
                _emit_terminal_output(terminal_callback, line_text, False)
                cleaned = strip_ansi(line_text).strip()
                if cleaned:
                    stdout_lines.append(cleaned)
                    last_cr_line = ""
                    if line_callback is not None:
                        try:
                            line_callback(cleaned, False)
                        except Exception:
                            pass
            elif cr_idx != -1:
                if cr_idx + 1 < len(raw_buf) and raw_buf[cr_idx + 1] == "\n":
                    line_text = raw_buf[:cr_idx]
                    raw_buf = raw_buf[cr_idx + 2 :]
                    _emit_terminal_output(terminal_callback, line_text, False)
                    cleaned = strip_ansi(line_text).strip()
                    if cleaned:
                        stdout_lines.append(cleaned)
                        last_cr_line = ""
                        if line_callback is not None:
                            try:
                                line_callback(cleaned, False)
                            except Exception:
                                pass
                else:
                    line_text = raw_buf[:cr_idx]
                    raw_buf = raw_buf[cr_idx + 1 :]
                    _emit_terminal_output(terminal_callback, line_text, True)
                    cleaned = strip_ansi(line_text).strip()
                    if cleaned and cleaned != last_cr_line:
                        last_cr_line = cleaned
                        if line_callback is not None:
                            try:
                                line_callback(cleaned, True)
                            except Exception:
                                pass
            else:
                break

    tail = strip_ansi(raw_buf).strip()
    if tail:
        stdout_lines.append(tail)
        _emit_terminal_output(terminal_callback, raw_buf, False)
        if line_callback is not None:
            try:
                line_callback(tail, False)
            except Exception:
                pass

    exit_code = proc.exitstatus if proc.exitstatus is not None else -1
    try:
        proc.close()
    except Exception:
        pass

    return exit_code, "\n".join(stdout_lines), ""


def run_scai_command(
    cmd: List[str],
    cwd: str,
    max_retries: int = 4,
    line_callback: Optional[Callable[[str, bool], None]] = None,
    terminal_callback: Optional[TerminalCallback] = None,
) -> tuple[int, str, str]:
    """Run a SCAI CLI command inside a PTY and return sanitized stdout."""
    return_code = -1
    stdout_str = ""
    stderr_str = ""

    for attempt in range(1, max_retries + 1):
        logger.debug(
            "[SCAI CMD] Executing attempt %s/%s: %s in %s",
            attempt,
            max_retries,
            " ".join(cmd),
            cwd,
        )

        return_code, stdout_str, stderr_str = run_scai_command_pty(
            cmd,
            cwd,
            line_callback=line_callback,
            terminal_callback=terminal_callback,
        )

        output_lower = stdout_str.lower() + stderr_str.lower()
        if return_code != 0 and (
            "license" in output_lower
            or "unauthorized" in output_lower
            or "unauthenticated" in output_lower
        ):
            logger.warning(
                "[SCAI CMD] License/Auth issue detected on attempt %s:\nSTDOUT: %s\nSTDERR: %s",
                attempt,
                stdout_str,
                stderr_str,
            )
            if attempt < max_retries:
                logger.info(
                    "[SCAI CMD] Retrying command in 2 seconds (attempt %s/%s)...",
                    attempt + 1,
                    max_retries,
                )
                time.sleep(2)
                continue

        logger.debug("[SCAI CMD] Attempt %s completed with code %s.", attempt, return_code)
        if return_code != 0:
            logger.error(
                "[SCAI CMD] Command failed with code %s.\nSTDOUT: %s\nSTDERR: %s",
                return_code,
                stdout_str,
                stderr_str,
            )
        return return_code, stdout_str, stderr_str

    return return_code, stdout_str, stderr_str
