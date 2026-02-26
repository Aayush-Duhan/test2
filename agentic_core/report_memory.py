from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .state import MigrationContext


DEFAULT_IGNORED_CODES_PATH = Path(__file__).resolve().parent / "config" / "ignored_report_codes.json"


def load_ignored_report_codes(config_path: Optional[Path] = None) -> List[str]:
    path = config_path or DEFAULT_IGNORED_CODES_PATH
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    codes = payload.get("ignored_codes", []) if isinstance(payload, dict) else []
    if not isinstance(codes, list):
        return []
    normalized = []
    for code in codes:
        if not isinstance(code, str):
            continue
        value = code.strip().upper()
        if value:
            normalized.append(value)
    return sorted(set(normalized))


def _find_latest(base: Path, pattern: str) -> Optional[Path]:
    files = list(base.glob(pattern))
    if not files:
        return None
    files.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return files[0]


def _parse_issues_csv(path: Optional[Path]) -> List[Dict[str, Any]]:
    if not path or not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            code = str(row.get("Code") or "").strip().upper()
            normalized: Dict[str, Any] = {
                "code": code,
                "severity": str(row.get("Severity") or "").strip(),
                "name": str(row.get("Name") or "").strip(),
                "description": str(row.get("Description") or "").strip(),
                "parent_file": str(row.get("ParentFile") or "").strip(),
                "line": str(row.get("Line") or "").strip(),
                "column": str(row.get("Column") or "").strip(),
                "migration_id": str(row.get("MigrationID") or "").strip(),
            }
            rows.append(normalized)
    return rows


def _parse_assessment_json(path: Optional[Path]) -> Dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    keys = [
        "AppVersion",
        "CoreVersion",
        "StartConversion",
        "ElapsedTime",
        "CodeCompletenessScore",
        "TotalFiles",
        "TotalWarnings",
        "TotalConversionErrors",
        "TotalParsingErrors",
        "TotalLinesOfCode",
        "TotalFDMs",
        "UniqueFDMs",
    ]
    return {key: payload.get(key) for key in keys}


def _extract_runtime_context(state: MigrationContext) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    latest_errors: List[Dict[str, Any]] = []
    failed_statements: List[Dict[str, Any]] = []

    for entry in (state.execution_errors or [])[-5:]:
        if not isinstance(entry, dict):
            continue
        latest_errors.append(
            {
                "type": entry.get("type"),
                "message": entry.get("message"),
                "object_name": entry.get("object_name"),
                "statement_index": entry.get("statement_index"),
            }
        )

    for file_entry in reversed(state.execution_log or []):
        if not isinstance(file_entry, dict):
            continue
        if file_entry.get("status") != "failed":
            continue
        failed_statements.append(
            {
                "file": file_entry.get("file"),
                "error_type": file_entry.get("error_type"),
                "error_message": file_entry.get("error_message"),
                "failed_statement": file_entry.get("failed_statement"),
                "failed_statement_index": file_entry.get("failed_statement_index"),
            }
        )
        if len(failed_statements) >= 3:
            break

    return latest_errors, failed_statements


def build_report_context_memory(state: MigrationContext) -> Dict[str, Any]:
    project_path = Path(state.project_path or "")
    snowconvert_reports = project_path / "converted" / "Reports" / "SnowConvert"

    issues_file = _find_latest(snowconvert_reports, "Issues.*.csv")
    assessment_file = _find_latest(snowconvert_reports, "Assessment.*.json")

    ignored_codes = load_ignored_report_codes()
    ignored_set = set(ignored_codes)

    all_issues = _parse_issues_csv(issues_file)
    actionable_issues: List[Dict[str, Any]] = []
    ignored_counter: Counter[str] = Counter()

    for issue in all_issues:
        code = str(issue.get("code") or "").strip().upper()
        if code and code in ignored_set:
            ignored_counter[code] += 1
            continue
        actionable_issues.append(issue)

    assessment_summary = _parse_assessment_json(assessment_file)
    latest_errors, failed_statements = _extract_runtime_context(state)

    prior_attempts = []
    for item in (state.self_heal_log or [])[-5:]:
        if not isinstance(item, dict):
            continue
        prior_attempts.append(
            {
                "iteration": item.get("iteration"),
                "success": item.get("success"),
                "issues_fixed": item.get("issues_fixed"),
                "error": item.get("error"),
            }
        )

    memory = {
        "reports_found": {
            "issues_csv": str(issues_file) if issues_file else "",
            "assessment_json": str(assessment_file) if assessment_file else "",
        },
        "assessment_summary": assessment_summary,
        "ignored_codes": ignored_codes,
        "report_scan_summary": {
            "total_report_issues": len(all_issues),
            "actionable_issues": len(actionable_issues),
            "ignored_issues": int(sum(ignored_counter.values())),
        },
        "ignored_issues_summary": dict(ignored_counter),
        "actionable_issues": actionable_issues[:25],
        "latest_execution_errors": latest_errors,
        "failed_statements": failed_statements,
        "prior_self_heal_attempts": prior_attempts,
    }
    return memory
