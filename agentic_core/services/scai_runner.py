"""CLI execution helpers for SCAI commands."""

from __future__ import annotations

import logging
import subprocess
import time
from typing import Callable, List, Optional

from agentic_core.utils.text import decode_cli_stream, strip_ansi

logger = logging.getLogger(__name__)


def run_scai_command_pty(
    cmd: List[str],
    cwd: str,
    line_callback: Callable[[str, bool], None],
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
        while "\n" in raw_buf or "\r" in raw_buf:
            cr_idx = raw_buf.find("\r")
            nl_idx = raw_buf.find("\n")

            if nl_idx != -1 and (cr_idx == -1 or nl_idx < cr_idx):
                line_text = raw_buf[:nl_idx]
                raw_buf = raw_buf[nl_idx + 1 :]
                cleaned = strip_ansi(line_text).strip()
                if cleaned:
                    stdout_lines.append(cleaned)
                    last_cr_line = ""
                    try:
                        line_callback(cleaned, False)
                    except Exception:
                        pass
            elif cr_idx != -1:
                if cr_idx + 1 < len(raw_buf) and raw_buf[cr_idx + 1] == "\n":
                    line_text = raw_buf[:cr_idx]
                    raw_buf = raw_buf[cr_idx + 2 :]
                    cleaned = strip_ansi(line_text).strip()
                    if cleaned:
                        stdout_lines.append(cleaned)
                        last_cr_line = ""
                        try:
                            line_callback(cleaned, False)
                        except Exception:
                            pass
                else:
                    line_text = raw_buf[:cr_idx]
                    raw_buf = raw_buf[cr_idx + 1 :]
                    cleaned = strip_ansi(line_text).strip()
                    if cleaned and cleaned != last_cr_line:
                        last_cr_line = cleaned
                        try:
                            line_callback(cleaned, True)
                        except Exception:
                            pass
            else:
                break

    tail = strip_ansi(raw_buf).strip()
    if tail:
        stdout_lines.append(tail)
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
) -> tuple[int, str, str]:
    """Run a CLI command and return decoded stdout/stderr."""
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

        if line_callback is not None:
            try:
                return_code, stdout_str, stderr_str = run_scai_command_pty(cmd, cwd, line_callback)
            except ImportError:
                logger.warning("[SCAI CMD] pywinpty not available, falling back to subprocess.Popen")
                proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout_lines: List[str] = []
                assert proc.stdout is not None
                for raw_line in proc.stdout:
                    decoded = decode_cli_stream(raw_line)
                    if decoded:
                        stdout_lines.append(decoded)
                        try:
                            line_callback(decoded, False)
                        except Exception:
                            pass
                assert proc.stderr is not None
                stderr_bytes = proc.stderr.read()
                proc.wait()
                stdout_str = "\n".join(stdout_lines)
                stderr_str = decode_cli_stream(stderr_bytes)
                return_code = proc.returncode
        else:
            result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=False)
            stdout_str = decode_cli_stream(result.stdout)
            stderr_str = decode_cli_stream(result.stderr)
            return_code = result.returncode

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
