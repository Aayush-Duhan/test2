import re

_ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
def strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences from terminal output."""
    return _ANSI_RE.sub("", text)
