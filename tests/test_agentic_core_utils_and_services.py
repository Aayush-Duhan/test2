import tempfile
import unittest
from pathlib import Path

from agentic_core.models.context import MigrationContext
from agentic_core.services.report_context import build_report_context_memory
from agentic_core.services.validation import validate_code
from agentic_core.utils.sql_files import list_sql_files, read_sql_files


class UtilsAndServicesTests(unittest.TestCase):
    def test_sql_file_helpers_read_and_sort_sql_like_files(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "b.sql").write_text("select 2;", encoding="utf-8")
            nested = root / "nested"
            nested.mkdir()
            (nested / "a.ddl").write_text("create table t(id int);", encoding="utf-8")

            self.assertEqual(
                list_sql_files(tmp_dir),
                [
                    str(root / "b.sql"),
                    str(nested / "a.ddl"),
                ],
            )
            contents = read_sql_files(tmp_dir)
            self.assertIn("-- FILE: b.sql", contents)
            self.assertIn("-- FILE: a.ddl", contents)

    def test_validate_code_flags_line_count_regression(self):
        result = validate_code(
            code="SELECT 1;",
            original_code="SELECT 1;\nSELECT 2;",
        )
        self.assertFalse(result.passed)
        self.assertEqual(result.issues[0]["type"], "line_count_regression")

    def test_report_context_loads_ignored_codes_and_reports(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            report_dir = Path(tmp_dir) / "converted" / "Reports" / "SnowConvert"
            report_dir.mkdir(parents=True)
            (report_dir / "Issues.001.csv").write_text(
                "Code,Severity,Name,Description,ParentFile,Line,Column,MigrationID\n"
                "SSC-FDM-0006,warning,name,ignored,file.sql,1,1,mid\n"
                "X100,error,name,actionable,file.sql,2,1,mid\n",
                encoding="utf-8",
            )
            (report_dir / "Assessment.001.json").write_text(
                '{"TotalFiles": 1, "TotalWarnings": 2}',
                encoding="utf-8",
            )
            state = MigrationContext(project_path=tmp_dir)

            memory = build_report_context_memory(state)

            self.assertIn("SSC-FDM-0006", memory["ignored_codes"])
            self.assertEqual(memory["report_scan_summary"]["total_report_issues"], 2)
            self.assertEqual(memory["report_scan_summary"]["actionable_issues"], 1)
