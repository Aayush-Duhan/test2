"""Shared text and terminal output helpers."""

import re

_ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def decode_cli_stream(data: bytes) -> str:
    """Decode CLI bytes robustly across Windows code pages."""
    if not data:
        return ""
    for encoding in ("utf-8", "utf-8-sig", "cp437", "cp1252", "latin-1"):
        try:
            return data.decode(encoding).strip()
        except Exception:
            continue
    return data.decode("utf-8", errors="replace").strip()


def strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences from terminal output."""
    return _ANSI_RE.sub("", text)
