"""Core file operations for partial viewing and editing of SQL files.

Used by the agent tools and the self-heal service to work within
the LLM's token output limit by operating on file sections instead
of full files.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import tempfile
from dataclasses import dataclass, field, replace
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024
DEFAULT_MAX_READ_BYTES = 512 * 1024
DEFAULT_MAX_LIST_ENTRIES = 5000
DEFAULT_MAX_SEARCH_RESULTS = 200
BINARY_CHECK_BYTES = 8192


@dataclass
class FileAccessPolicy:
    root_paths: List[str] = field(default_factory=list)
    allow_hidden: bool = False
    allowed_extensions: Optional[set[str]] = None
    max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES
    max_read_bytes: int = DEFAULT_MAX_READ_BYTES
    max_list_entries: int = DEFAULT_MAX_LIST_ENTRIES
    max_search_results: int = DEFAULT_MAX_SEARCH_RESULTS
    allow_binary: bool = False
    follow_symlinks: bool = True

    def normalized_roots(self) -> List[str]:
        roots = [os.path.abspath(path) for path in self.root_paths if path]
        if self.follow_symlinks:
            roots = [os.path.realpath(path) for path in roots]
        return roots


def _normalize_extensions(allowed_extensions: Optional[set[str]]) -> Optional[set[str]]:
    if not allowed_extensions:
        return None
    normalized: set[str] = set()
    for ext in allowed_extensions:
        if not ext:
            continue
        ext_lower = ext.lower()
        if not ext_lower.startswith("."):
            ext_lower = f".{ext_lower}"
        normalized.add(ext_lower)
    return normalized


def _path_is_hidden(path: str, root: Optional[str] = None) -> bool:
    try:
        if root:
            rel_path = os.path.relpath(path, root)
        else:
            rel_path = path
    except ValueError:
        rel_path = path

    for part in Path(rel_path).parts:
        if part.startswith("."):
            return True
    return False


def _resolve_path(
    raw_path: str,
    policy: Optional[FileAccessPolicy],
    *,
    must_exist: Optional[bool] = None,
    allow_dir: bool = False,
    allow_file: bool = True,
) -> Tuple[str, Optional[str]]:
    if not raw_path:
        raise ValueError("path is required")

    roots = policy.normalized_roots() if policy else []
    candidate = raw_path

    if roots and not os.path.isabs(candidate):
        candidate = os.path.join(roots[0], candidate)

    absolute = os.path.abspath(candidate)
    resolved = os.path.realpath(absolute) if policy and policy.follow_symlinks else absolute

    root_used: Optional[str] = None
    if roots:
        for root in roots:
            try:
                if os.path.commonpath([root, resolved]) == root:
                    root_used = root
                    break
            except ValueError:
                continue
        if root_used is None:
            raise ValueError("Path is outside allowed roots")

    if policy and not policy.allow_hidden:
        root_for_hidden = root_used or (roots[0] if roots else None)
        if _path_is_hidden(resolved, root_for_hidden):
            raise ValueError("Hidden paths are not allowed")

    if must_exist is True and not os.path.exists(resolved):
        raise FileNotFoundError(f"File not found: {resolved}")

    if os.path.exists(resolved):
        if os.path.isdir(resolved) and not allow_dir:
            raise ValueError("Expected a file, but got a directory")
        if os.path.isfile(resolved) and not allow_file:
            raise ValueError("Expected a directory, but got a file")
        if policy and policy.allowed_extensions and os.path.isfile(resolved):
            allowed = _normalize_extensions(policy.allowed_extensions)
            ext = Path(resolved).suffix.lower()
            if allowed and ext and ext not in allowed:
                raise ValueError("File extension not allowed")

    return resolved, root_used


def _is_binary_file(path: str) -> bool:
    try:
        with open(path, "rb") as handle:
            chunk = handle.read(BINARY_CHECK_BYTES)
        return b"\x00" in chunk
    except Exception:
        return False


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: str) -> str:
    sha256 = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def _write_atomic(path: str, lines: List[str]) -> None:
    directory = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.writelines(lines)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def get_file_info(
    file_path: str,
    *,
    policy: Optional[FileAccessPolicy] = None,
    include_hash: bool = False,
) -> Dict[str, Any]:
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
    if not file_path:
        return result

    try:
        resolved, root_used = _resolve_path(file_path, policy, must_exist=False, allow_dir=False)
    except (ValueError, FileNotFoundError) as exc:
        result["error"] = str(exc)
        return result

    if not os.path.isfile(resolved):
        return result

    result["file_path"] = resolved
    result["exists"] = True
    size_bytes = os.path.getsize(resolved)
    result["size_bytes"] = size_bytes
    result["is_binary"] = _is_binary_file(resolved)

    too_large = False
    if policy and policy.max_file_size_bytes and size_bytes > policy.max_file_size_bytes:
        too_large = True
    result["too_large"] = too_large

    if not too_large and not result["is_binary"]:
        try:
            with open(resolved, "r", encoding="utf-8", errors="replace") as handle:
                total_lines = sum(1 for _ in handle)
            result["total_lines"] = total_lines
        except Exception as exc:
            result["error"] = f"Failed to read file for line count: {exc}"
            result["total_lines"] = 0

    if include_hash:
        try:
            result["sha256"] = _sha256_file(resolved)
        except Exception as exc:
            result["hash_error"] = str(exc)

    if root_used:
        try:
            result["relative_path"] = os.path.relpath(resolved, root_used)
        except ValueError:
            pass

    return result


def view_file_section(
    file_path: str,
    start_line: int = 1,
    end_line: Optional[int] = None,
    *,
    default_window: int = 100,
    policy: Optional[FileAccessPolicy] = None,
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
    if not file_path:
        return {
            "error": "file_path is required",
            "file_path": file_path,
            "content": "",
            "total_lines": 0,
        }

    try:
        resolved, root_used = _resolve_path(file_path, policy, must_exist=True, allow_dir=False)
    except (ValueError, FileNotFoundError) as exc:
        return {
            "error": str(exc),
            "file_path": file_path,
            "content": "",
            "total_lines": 0,
        }

    if policy and not policy.allow_binary and _is_binary_file(resolved):
        return {
            "error": "Binary files are not allowed",
            "file_path": resolved,
            "content": "",
            "total_lines": 0,
        }

    file_size = os.path.getsize(resolved)
    if policy and policy.max_file_size_bytes and file_size > policy.max_file_size_bytes:
        return {
            "error": "File exceeds max_file_size_bytes",
            "file_path": resolved,
            "content": "",
            "total_lines": 0,
            "size_bytes": file_size,
            "max_file_size_bytes": policy.max_file_size_bytes,
        }

    start_line = max(1, int(start_line))
    if end_line is None:
        end_line = start_line + max(1, default_window) - 1
    end_line = max(start_line, int(end_line))

    numbered: List[str] = []
    total_lines = 0
    bytes_read = 0
    truncated = False
    last_line_returned = 0
    capture = True

    try:
        with open(resolved, "r", encoding="utf-8", errors="replace") as handle:
            for line_no, line in enumerate(handle, start=1):
                total_lines += 1
                if line_no < start_line:
                    continue
                if line_no > end_line:
                    continue

                if capture and policy and policy.max_read_bytes:
                    line_bytes = len(line.encode("utf-8"))
                    if bytes_read + line_bytes > policy.max_read_bytes:
                        truncated = True
                        capture = False
                        continue
                    bytes_read += line_bytes

                if capture:
                    numbered.append(f"{line_no}: {line.rstrip()}")
                    last_line_returned = line_no
    except Exception as exc:
        return {
            "error": f"Failed to read file: {exc}",
            "file_path": resolved,
            "content": "",
            "total_lines": 0,
        }

    if total_lines == 0:
        return {
            "file_path": resolved,
            "content": "",
            "start_line": 1,
            "end_line": 0,
            "total_lines": 0,
        }

    reported_end = last_line_returned if last_line_returned else min(end_line, total_lines)
    result: Dict[str, Any] = {
        "file_path": resolved,
        "content": "\n".join(numbered),
        "start_line": start_line,
        "end_line": reported_end,
        "total_lines": total_lines,
        "truncated": truncated,
    }

    if root_used:
        try:
            result["relative_path"] = os.path.relpath(resolved, root_used)
        except ValueError:
            pass

    return result


def edit_file_section(
    file_path: str,
    start_line: int,
    end_line: int,
    new_content: str,
    *,
    expected_hash: Optional[str] = None,
    policy: Optional[FileAccessPolicy] = None,
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
    if not file_path:
        return {"success": False, "error": "file_path is required"}

    try:
        resolved, root_used = _resolve_path(file_path, policy, must_exist=True, allow_dir=False)
    except (ValueError, FileNotFoundError) as exc:
        return {"success": False, "error": str(exc)}

    file_size = os.path.getsize(resolved)
    if policy and policy.max_file_size_bytes and file_size > policy.max_file_size_bytes:
        return {"success": False, "error": "File exceeds max_file_size_bytes"}

    try:
        with open(resolved, "rb") as handle:
            raw = handle.read()
    except Exception as exc:
        return {"success": False, "error": f"Failed to read file: {exc}"}

    if policy and not policy.allow_binary and b"\x00" in raw[:BINARY_CHECK_BYTES]:
        return {"success": False, "error": "Binary files are not allowed"}

    current_hash = _sha256_bytes(raw)
    if expected_hash and expected_hash != current_hash:
        return {
            "success": False,
            "error": "File hash does not match expected_hash",
            "current_hash": current_hash,
        }

    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines(keepends=True)
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

    end_line = min(end_line, total_lines)

    new_lines_raw = new_content.split("\n") if new_content else []
    new_lines = []
    for line in new_lines_raw:
        if not line.endswith("\n"):
            line += "\n"
        new_lines.append(line)

    lines_removed = end_line - start_line + 1 if end_line >= start_line else 0
    before = lines[: start_line - 1]
    after = lines[end_line:]
    result_lines = before + new_lines + after

    try:
        _write_atomic(resolved, result_lines)
    except Exception as exc:
        return {"success": False, "error": f"Failed to write file: {exc}"}

    result: Dict[str, Any] = {
        "success": True,
        "file_path": resolved,
        "lines_removed": lines_removed,
        "lines_added": len(new_lines),
        "new_total_lines": len(result_lines),
        "new_hash": _sha256_bytes("".join(result_lines).encode("utf-8")),
    }

    if root_used:
        try:
            result["relative_path"] = os.path.relpath(resolved, root_used)
        except ValueError:
            pass

    return result


def apply_edit_operations(
    file_path: str,
    edits: List[Dict[str, Any]],
    *,
    expected_hash: Optional[str] = None,
    policy: Optional[FileAccessPolicy] = None,
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
    if not file_path:
        return {"success": False, "error": "file_path is required"}

    try:
        resolved, root_used = _resolve_path(file_path, policy, must_exist=True, allow_dir=False)
    except (ValueError, FileNotFoundError) as exc:
        return {"success": False, "error": str(exc)}

    file_size = os.path.getsize(resolved)
    if policy and policy.max_file_size_bytes and file_size > policy.max_file_size_bytes:
        return {"success": False, "error": "File exceeds max_file_size_bytes"}

    if not edits:
        return {"success": True, "edits_applied": 0, "message": "No edits provided"}

    try:
        with open(resolved, "rb") as handle:
            raw = handle.read()
    except Exception as exc:
        return {"success": False, "error": f"Failed to read file: {exc}"}

    if policy and not policy.allow_binary and b"\x00" in raw[:BINARY_CHECK_BYTES]:
        return {"success": False, "error": "Binary files are not allowed"}

    current_hash = _sha256_bytes(raw)
    if expected_hash and expected_hash != current_hash:
        return {
            "success": False,
            "error": "File hash does not match expected_hash",
            "current_hash": current_hash,
        }

    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines(keepends=True)
    total_lines = len(lines)

    normalized_edits: List[Dict[str, Any]] = []
    for index, edit in enumerate(edits):
        start = edit.get("start_line")
        end = edit.get("end_line")
        content = edit.get("new_content", "")
        if start is None or end is None:
            return {"success": False, "error": "Missing start_line or end_line", "edit": edit}
        try:
            start_int = int(start)
            end_int = int(end)
        except (TypeError, ValueError):
            return {"success": False, "error": "start_line and end_line must be integers", "edit": edit}

        if start_int < 1 or start_int > total_lines + 1:
            return {
                "success": False,
                "error": f"start_line {start_int} out of range (file has {total_lines} lines)",
            }
        if end_int < start_int - 1:
            return {
                "success": False,
                "error": f"end_line {end_int} must be >= start_line - 1 ({start_int - 1})",
            }

        normalized_edits.append(
            {
                "index": index,
                "start_line": start_int,
                "end_line": end_int,
                "new_content": str(content),
            }
        )

    # Detect overlapping replacement ranges
    replacement_ranges = sorted(
        [(e["start_line"], e["end_line"]) for e in normalized_edits if e["end_line"] >= e["start_line"]],
        key=lambda item: item[0],
    )
    for idx in range(1, len(replacement_ranges)):
        prev_start, prev_end = replacement_ranges[idx - 1]
        curr_start, curr_end = replacement_ranges[idx]
        if curr_start <= prev_end:
            return {"success": False, "error": "Overlapping edit ranges are not allowed"}

    # Ensure insertions do not fall inside replacement ranges
    for edit in normalized_edits:
        if edit["end_line"] >= edit["start_line"]:
            continue
        insert_at = edit["start_line"]
        for start, end in replacement_ranges:
            if start <= insert_at <= end:
                return {"success": False, "error": "Insertion overlaps a replacement range"}

    # Apply edits bottom-up, preserving insertion order for same line
    sorted_edits = sorted(
        normalized_edits,
        key=lambda e: (e["start_line"], e["index"]),
        reverse=True,
    )

    result_lines = lines
    lines_removed_total = 0
    lines_added_total = 0
    details: List[Dict[str, Any]] = []

    for edit in sorted_edits:
        start = edit["start_line"]
        end = min(edit["end_line"], len(result_lines))
        content = edit["new_content"]
        new_lines_raw = content.split("\n") if content else []
        new_lines = []
        for line in new_lines_raw:
            if not line.endswith("\n"):
                line += "\n"
            new_lines.append(line)

        lines_removed = 0
        if end >= start:
            lines_removed = end - start + 1
            lines_removed_total += lines_removed
            result_lines[start - 1 : end] = new_lines
        else:
            result_lines[start - 1 : start - 1] = new_lines
        lines_added = len(new_lines)
        lines_added_total += lines_added
        details.append(
            {
                "start_line": start,
                "end_line": end,
                "lines_removed": lines_removed,
                "lines_added": lines_added,
                "success": True,
            }
        )

    try:
        _write_atomic(resolved, result_lines)
    except Exception as exc:
        return {"success": False, "error": f"Failed to write file: {exc}"}

    result: Dict[str, Any] = {
        "success": True,
        "file_path": resolved,
        "edits_applied": len(normalized_edits),
        "total_edits": len(normalized_edits),
        "lines_removed": lines_removed_total,
        "lines_added": lines_added_total,
        "new_total_lines": len(result_lines),
        "new_hash": _sha256_bytes("".join(result_lines).encode("utf-8")),
        "details": details,
    }

    if root_used:
        try:
            result["relative_path"] = os.path.relpath(resolved, root_used)
        except ValueError:
            pass

    return result


def list_directory(
    dir_path: str,
    *,
    policy: Optional[FileAccessPolicy] = None,
    max_depth: int = 2,
    pattern: Optional[str] = None,
    include_files: bool = True,
    include_dirs: bool = True,
    include_hidden: Optional[bool] = None,
) -> Dict[str, Any]:
    """List directory entries with optional depth and filtering."""
    effective_policy = policy
    if include_hidden is not None and policy:
        effective_policy = replace(policy, allow_hidden=include_hidden)

    if not dir_path:
        return {"error": "dir_path is required", "entries": []}

    try:
        resolved, root_used = _resolve_path(dir_path, effective_policy, must_exist=True, allow_dir=True, allow_file=False)
    except (ValueError, FileNotFoundError) as exc:
        return {"error": str(exc), "entries": []}

    if not os.path.isdir(resolved):
        return {"error": "Not a directory", "entries": []}

    max_depth = max(0, int(max_depth))
    max_entries = effective_policy.max_list_entries if effective_policy else DEFAULT_MAX_LIST_ENTRIES
    allow_hidden = effective_policy.allow_hidden if effective_policy else False

    entries: List[Dict[str, Any]] = []
    truncated = False

    base_depth = resolved.count(os.sep)

    for current_root, dirs, files in os.walk(resolved):
        current_depth = current_root.count(os.sep) - base_depth
        if current_depth > max_depth:
            dirs[:] = []
            continue

        if not allow_hidden:
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            files = [f for f in files if not f.startswith(".")]

        if pattern:
            dirs[:] = [d for d in dirs if fnmatch(d, pattern)]
            files = [f for f in files if fnmatch(f, pattern)]

        if include_dirs:
            for dirname in dirs:
                entry_path = os.path.join(current_root, dirname)
                entries.append({"path": entry_path, "type": "dir"})
                if len(entries) >= max_entries:
                    truncated = True
                    break

        if truncated:
            break

        if include_files:
            for filename in files:
                entry_path = os.path.join(current_root, filename)
                try:
                    size_bytes = os.path.getsize(entry_path)
                except OSError:
                    size_bytes = 0
                entries.append({"path": entry_path, "type": "file", "size_bytes": size_bytes})
                if len(entries) >= max_entries:
                    truncated = True
                    break

        if truncated:
            break

    result: Dict[str, Any] = {"entries": entries, "truncated": truncated}
    if root_used:
        try:
            result["root"] = root_used
        except ValueError:
            pass

    return result


def search_in_file(
    file_path: str,
    query: str,
    *,
    policy: Optional[FileAccessPolicy] = None,
    regex: bool = False,
    case_sensitive: bool = False,
    max_results: Optional[int] = None,
) -> Dict[str, Any]:
    """Search for a string or regex in a file."""
    if not file_path:
        return {"error": "file_path is required", "matches": []}
    if not query:
        return {"error": "query is required", "matches": []}

    try:
        resolved, root_used = _resolve_path(file_path, policy, must_exist=True, allow_dir=False)
    except (ValueError, FileNotFoundError) as exc:
        return {"error": str(exc), "matches": []}

    if policy and not policy.allow_binary and _is_binary_file(resolved):
        return {"error": "Binary files are not allowed", "matches": []}

    limit = max_results
    if limit is None:
        limit = policy.max_search_results if policy else DEFAULT_MAX_SEARCH_RESULTS

    flags = 0 if case_sensitive else re.IGNORECASE
    pattern = re.compile(query, flags) if regex else None

    matches: List[Dict[str, Any]] = []
    truncated = False

    try:
        with open(resolved, "r", encoding="utf-8", errors="replace") as handle:
            for line_no, line in enumerate(handle, start=1):
                haystack = line if case_sensitive else line.lower()
                found = False
                if regex and pattern:
                    if pattern.search(line):
                        found = True
                else:
                    needle = query if case_sensitive else query.lower()
                    if needle in haystack:
                        found = True

                if found:
                    line_text = line.rstrip("\n")
                    if len(line_text) > 500:
                        line_text = line_text[:500] + "..."
                    matches.append({"line": line_no, "text": line_text})
                    if len(matches) >= limit:
                        truncated = True
                        break
    except Exception as exc:
        return {"error": f"Failed to search file: {exc}", "matches": []}

    result: Dict[str, Any] = {"file_path": resolved, "matches": matches, "truncated": truncated}
    if root_used:
        try:
            result["relative_path"] = os.path.relpath(resolved, root_used)
        except ValueError:
            pass

    return result


def read_file(
    file_path: str,
    *,
    policy: Optional[FileAccessPolicy] = None,
    max_bytes: Optional[int] = None,
) -> Dict[str, Any]:
    """Read file contents with size limits."""
    if not file_path:
        return {"error": "file_path is required", "content": ""}

    try:
        resolved, root_used = _resolve_path(file_path, policy, must_exist=True, allow_dir=False)
    except (ValueError, FileNotFoundError) as exc:
        return {"error": str(exc), "content": ""}

    if policy and not policy.allow_binary and _is_binary_file(resolved):
        return {"error": "Binary files are not allowed", "content": "", "is_binary": True}

    size_bytes = os.path.getsize(resolved)
    if policy and policy.max_file_size_bytes and size_bytes > policy.max_file_size_bytes:
        return {"error": "File exceeds max_file_size_bytes", "content": "", "size_bytes": size_bytes}

    if max_bytes is None and policy:
        max_bytes = policy.max_read_bytes

    try:
        with open(resolved, "rb") as handle:
            if max_bytes:
                data = handle.read(max_bytes)
                truncated = handle.read(1) != b""
            else:
                data = handle.read()
                truncated = False
    except Exception as exc:
        return {"error": f"Failed to read file: {exc}", "content": ""}

    result: Dict[str, Any] = {
        "file_path": resolved,
        "content": data.decode("utf-8", errors="replace"),
        "truncated": truncated,
        "size_bytes": size_bytes,
    }

    if root_used:
        try:
            result["relative_path"] = os.path.relpath(resolved, root_used)
        except ValueError:
            pass

    return result


def write_file_content(
    file_path: str,
    content: str,
    *,
    policy: Optional[FileAccessPolicy] = None,
    expected_hash: Optional[str] = None,
    create_dirs: bool = True,
) -> Dict[str, Any]:
    """Write file contents atomically, optionally with hash guard."""
    if not file_path:
        return {"success": False, "error": "file_path is required"}

    try:
        resolved, root_used = _resolve_path(file_path, policy, must_exist=False, allow_dir=False)
    except (ValueError, FileNotFoundError) as exc:
        return {"success": False, "error": str(exc)}

    if policy and policy.allowed_extensions:
        allowed = _normalize_extensions(policy.allowed_extensions)
        ext = Path(resolved).suffix.lower()
        if allowed and ext and ext not in allowed:
            return {"success": False, "error": "File extension not allowed"}

    if os.path.exists(resolved) and expected_hash:
        try:
            current_hash = _sha256_file(resolved)
        except Exception as exc:
            return {"success": False, "error": f"Failed to hash file: {exc}"}
        if expected_hash != current_hash:
            return {"success": False, "error": "File hash does not match expected_hash", "current_hash": current_hash}

    if create_dirs:
        directory = os.path.dirname(resolved)
        os.makedirs(directory, exist_ok=True)

    lines = content.split("\n") if content else []
    normalized_lines: List[str] = []
    for line in lines:
        if not line.endswith("\n"):
            line += "\n"
        normalized_lines.append(line)

    try:
        _write_atomic(resolved, normalized_lines)
    except Exception as exc:
        return {"success": False, "error": f"Failed to write file: {exc}"}

    written_content = "".join(normalized_lines)
    result: Dict[str, Any] = {
        "success": True,
        "file_path": resolved,
        "bytes_written": len(written_content.encode("utf-8")),
        "new_hash": _sha256_bytes(written_content.encode("utf-8")),
    }

    if root_used:
        try:
            result["relative_path"] = os.path.relpath(resolved, root_used)
        except ValueError:
            pass

    return result


def make_directory(
    dir_path: str,
    *,
    policy: Optional[FileAccessPolicy] = None,
) -> Dict[str, Any]:
    """Create a directory safely under the allowed roots."""
    if not dir_path:
        return {"success": False, "error": "dir_path is required"}

    try:
        resolved, root_used = _resolve_path(dir_path, policy, must_exist=False, allow_dir=True, allow_file=False)
    except (ValueError, FileNotFoundError) as exc:
        return {"success": False, "error": str(exc)}

    try:
        os.makedirs(resolved, exist_ok=True)
    except Exception as exc:
        return {"success": False, "error": f"Failed to create directory: {exc}"}

    result: Dict[str, Any] = {"success": True, "path": resolved}
    if root_used:
        try:
            result["relative_path"] = os.path.relpath(resolved, root_used)
        except ValueError:
            pass

    return result
