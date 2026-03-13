"""Core file operations for partial viewing and editing of SQL files.

Used by the agent tools and the self-heal service to work within
the LLM's token output limit by operating on file sections instead
of full files.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def get_file_info(file_path: str) -> Dict[str, Any]:
    """Return metadata about a file (total lines, size in bytes).

    Args:
        file_path: Absolute path to the file.

    Returns:
        Dict with keys: file_path, total_lines, size_bytes, exists.
    """
    result: Dict[str, Any] = {
        "file_path": file_path,
        "exists": False,
        "total_lines": 0,
        "size_bytes": 0,
    }
    if not file_path or not os.path.isfile(file_path):
        return result

    result["exists"] = True
    result["size_bytes"] = os.path.getsize(file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    result["total_lines"] = len(lines)
    return result


def view_file_section(
    file_path: str,
    start_line: int = 1,
    end_line: Optional[int] = None,
    *,
    default_window: int = 100,
) -> Dict[str, Any]:
    """Return a section of a file with 1-indexed line numbers.

    Args:
        file_path: Absolute path to the file.
        start_line: First line to return (1-indexed, inclusive).
        end_line: Last line to return (1-indexed, inclusive).
                  If None, defaults to start_line + default_window - 1.
        default_window: Number of lines when end_line is not specified.

    Returns:
        Dict with keys: content (numbered lines), start_line, end_line,
        total_lines, file_path.
    """
    if not file_path or not os.path.isfile(file_path):
        return {
            "error": f"File not found: {file_path}",
            "file_path": file_path,
            "content": "",
            "total_lines": 0,
        }

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    total_lines = len(lines)
    if total_lines == 0:
        return {
            "file_path": file_path,
            "content": "",
            "start_line": 1,
            "end_line": 0,
            "total_lines": 0,
        }

    # Clamp bounds
    start_line = max(1, start_line)
    if end_line is None:
        end_line = min(start_line + default_window - 1, total_lines)
    end_line = min(end_line, total_lines)
    end_line = max(end_line, start_line)

    # Build numbered output (1-indexed)
    selected = lines[start_line - 1 : end_line]
    numbered = []
    for i, line in enumerate(selected, start=start_line):
        # Strip trailing newline for cleaner display
        numbered.append(f"{i}: {line.rstrip()}")

    return {
        "file_path": file_path,
        "content": "\n".join(numbered),
        "start_line": start_line,
        "end_line": end_line,
        "total_lines": total_lines,
    }


def edit_file_section(
    file_path: str,
    start_line: int,
    end_line: int,
    new_content: str,
) -> Dict[str, Any]:
    """Replace lines [start_line, end_line] (1-indexed, inclusive) with new_content.

    Args:
        file_path: Absolute path to the file.
        start_line: First line to replace (1-indexed, inclusive).
        end_line: Last line to replace (1-indexed, inclusive).
        new_content: Replacement text (may be more or fewer lines than
                     the range being replaced).

    Returns:
        Dict with keys: success, lines_removed, lines_added, new_total_lines.
    """
    if not file_path or not os.path.isfile(file_path):
        return {"success": False, "error": f"File not found: {file_path}"}

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    total_lines = len(lines)

    # Validate range
    if start_line < 1 or start_line > total_lines + 1:
        return {
            "success": False,
            "error": f"start_line {start_line} out of range (file has {total_lines} lines)",
        }
    if end_line < start_line - 1:
        return {
            "success": False,
            "error": f"end_line {end_line} must be >= start_line - 1 ({start_line - 1})",
        }
    # Allow end_line == start_line - 1 for pure insertion (insert before start_line)
    end_line = min(end_line, total_lines)

    # Prepare new lines — ensure each line ends with \n
    new_lines_raw = new_content.split("\n") if new_content else []
    new_lines = []
    for line in new_lines_raw:
        if not line.endswith("\n"):
            line += "\n"
        new_lines.append(line)

    # Replace: lines[start_line-1 : end_line] → new_lines
    lines_removed = end_line - start_line + 1 if end_line >= start_line else 0
    before = lines[: start_line - 1]
    after = lines[end_line:]
    result_lines = before + new_lines + after

    with open(file_path, "w", encoding="utf-8") as f:
        f.writelines(result_lines)

    return {
        "success": True,
        "file_path": file_path,
        "lines_removed": lines_removed,
        "lines_added": len(new_lines),
        "new_total_lines": len(result_lines),
    }


def apply_edit_operations(
    file_path: str,
    edits: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Apply multiple edit operations to a file.

    Edits are sorted in reverse line order so that line numbers remain
    valid as earlier edits shift content.

    Args:
        file_path: Absolute path to the file.
        edits: List of dicts with keys: start_line, end_line, new_content.

    Returns:
        Dict with success status and summary.
    """
    if not file_path or not os.path.isfile(file_path):
        return {"success": False, "error": f"File not found: {file_path}"}

    if not edits:
        return {"success": True, "edits_applied": 0, "message": "No edits provided"}

    # Sort edits by start_line descending so we apply bottom-up
    sorted_edits = sorted(edits, key=lambda e: e.get("start_line", 0), reverse=True)

    results = []
    for edit in sorted_edits:
        start = edit.get("start_line")
        end = edit.get("end_line")
        content = edit.get("new_content", "")

        if start is None or end is None:
            results.append({"success": False, "error": "Missing start_line or end_line", "edit": edit})
            continue

        result = edit_file_section(file_path, int(start), int(end), str(content))
        results.append(result)

    all_success = all(r.get("success", False) for r in results)
    return {
        "success": all_success,
        "edits_applied": sum(1 for r in results if r.get("success")),
        "total_edits": len(edits),
        "details": results,
    }
