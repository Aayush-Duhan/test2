"""Tests for the file tools service (view, edit, info, batch operations)."""

import os
import shutil
import tempfile
import textwrap
import unittest

from agentic_core.services.file_tools import (
    apply_edit_operations,
    edit_file_section,
    get_file_info,
    view_file_section,
)


class TestGetFileInfo(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.filepath = os.path.join(self.tmpdir, "test.sql")
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.write("SELECT 1;\nSELECT 2;\nSELECT 3;\n")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_existing_file(self):
        info = get_file_info(self.filepath)
        self.assertTrue(info["exists"])
        self.assertEqual(info["total_lines"], 3)
        self.assertGreater(info["size_bytes"], 0)

    def test_missing_file(self):
        info = get_file_info("/nonexistent/path.sql")
        self.assertFalse(info["exists"])
        self.assertEqual(info["total_lines"], 0)

    def test_empty_path(self):
        info = get_file_info("")
        self.assertFalse(info["exists"])


class TestViewFileSection(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.filepath = os.path.join(self.tmpdir, "test.sql")
        lines = [f"LINE_{i}\n" for i in range(1, 11)]  # 10 lines
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.writelines(lines)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_view_first_five_lines(self):
        result = view_file_section(self.filepath, 1, 5)
        self.assertEqual(result["start_line"], 1)
        self.assertEqual(result["end_line"], 5)
        self.assertEqual(result["total_lines"], 10)
        self.assertIn("1: LINE_1", result["content"])
        self.assertIn("5: LINE_5", result["content"])
        self.assertNotIn("6:", result["content"])

    def test_view_middle(self):
        result = view_file_section(self.filepath, 3, 7)
        self.assertIn("3: LINE_3", result["content"])
        self.assertIn("7: LINE_7", result["content"])
        self.assertNotIn("2:", result["content"])

    def test_clamp_to_file_bounds(self):
        result = view_file_section(self.filepath, 8, 20)
        self.assertEqual(result["end_line"], 10)  # Clamped
        self.assertIn("10: LINE_10", result["content"])

    def test_default_window(self):
        result = view_file_section(self.filepath, 1, None, default_window=3)
        self.assertEqual(result["end_line"], 3)

    def test_missing_file(self):
        result = view_file_section("/nonexistent.sql")
        self.assertIn("error", result)

    def test_empty_file(self):
        empty = os.path.join(self.tmpdir, "empty.sql")
        open(empty, "w").close()
        result = view_file_section(empty)
        self.assertEqual(result["total_lines"], 0)


class TestEditFileSection(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.filepath = os.path.join(self.tmpdir, "test.sql")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _write(self, content):
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.write(content)

    def _read(self):
        with open(self.filepath, "r", encoding="utf-8") as f:
            return f.read()

    def test_replace_single_line(self):
        self._write("line1\nline2\nline3\n")
        result = edit_file_section(self.filepath, 2, 2, "REPLACED")
        self.assertTrue(result["success"])
        self.assertEqual(result["lines_removed"], 1)
        self.assertEqual(result["lines_added"], 1)
        content = self._read()
        self.assertIn("REPLACED", content)
        self.assertIn("line1", content)
        self.assertIn("line3", content)

    def test_replace_multiple_lines(self):
        self._write("line1\nline2\nline3\nline4\nline5\n")
        result = edit_file_section(self.filepath, 2, 4, "NEW_A\nNEW_B")
        self.assertTrue(result["success"])
        self.assertEqual(result["lines_removed"], 3)
        self.assertEqual(result["lines_added"], 2)
        content = self._read()
        self.assertIn("line1", content)
        self.assertIn("NEW_A", content)
        self.assertIn("NEW_B", content)
        self.assertIn("line5", content)
        self.assertNotIn("line2", content)

    def test_replace_first_line(self):
        self._write("line1\nline2\nline3\n")
        result = edit_file_section(self.filepath, 1, 1, "FIRST")
        self.assertTrue(result["success"])
        lines = self._read().splitlines()
        self.assertEqual(lines[0], "FIRST")

    def test_replace_last_line(self):
        self._write("line1\nline2\nline3\n")
        result = edit_file_section(self.filepath, 3, 3, "LAST")
        self.assertTrue(result["success"])
        lines = self._read().splitlines()
        self.assertEqual(lines[2], "LAST")

    def test_expand_content(self):
        """Replacing 1 line with 3 lines should increase file size."""
        self._write("line1\nline2\nline3\n")
        result = edit_file_section(self.filepath, 2, 2, "A\nB\nC")
        self.assertTrue(result["success"])
        self.assertEqual(result["new_total_lines"], 5)

    def test_shrink_content(self):
        """Replacing 3 lines with 1 line should decrease file size."""
        self._write("line1\nline2\nline3\nline4\nline5\n")
        result = edit_file_section(self.filepath, 2, 4, "SINGLE")
        self.assertTrue(result["success"])
        self.assertEqual(result["new_total_lines"], 3)

    def test_invalid_range(self):
        self._write("line1\nline2\n")
        result = edit_file_section(self.filepath, 5, 10, "X")
        self.assertFalse(result["success"])

    def test_missing_file(self):
        result = edit_file_section("/nonexistent.sql", 1, 1, "X")
        self.assertFalse(result["success"])


class TestApplyEditOperations(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.filepath = os.path.join(self.tmpdir, "test.sql")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _write(self, content):
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.write(content)

    def _read(self):
        with open(self.filepath, "r", encoding="utf-8") as f:
            return f.read()

    def test_multiple_edits_bottom_up(self):
        """Multiple non-overlapping edits should apply correctly."""
        self._write("line1\nline2\nline3\nline4\nline5\n")
        edits = [
            {"start_line": 2, "end_line": 2, "new_content": "REPLACED_2"},
            {"start_line": 4, "end_line": 4, "new_content": "REPLACED_4"},
        ]
        result = apply_edit_operations(self.filepath, edits)
        self.assertTrue(result["success"])
        self.assertEqual(result["edits_applied"], 2)
        content = self._read()
        self.assertIn("REPLACED_2", content)
        self.assertIn("REPLACED_4", content)
        self.assertIn("line1", content)
        self.assertIn("line3", content)
        self.assertIn("line5", content)

    def test_empty_edits(self):
        self._write("line1\n")
        result = apply_edit_operations(self.filepath, [])
        self.assertTrue(result["success"])
        self.assertEqual(result["edits_applied"], 0)

    def test_missing_file(self):
        result = apply_edit_operations("/nonexistent.sql", [{"start_line": 1, "end_line": 1, "new_content": "X"}])
        self.assertFalse(result["success"])


class TestSelfHealEditParsing(unittest.TestCase):
    """Test that the JSON edit format used by self-heal is correctly applied."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.filepath = os.path.join(self.tmpdir, "converted.sql")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_simulated_self_heal_edit(self):
        """Simulates what happens when self-heal returns JSON edits."""
        import json

        # Original code (10 lines)
        original = textwrap.dedent("""\
            CREATE TABLE ODS.CUSTOMERS (
                CUSTOMER_ID NUMBER(10) NOT NULL,
                CUSTOMER_NAME VARCHAR2(150) NOT NULL,
                EMAIL_ID VARCHAR2(200),
                STATUS_FLAG CHAR(1) DEFAULT 'A',
                CREATED_DATE DATE DEFAULT SYSDATE,
                CONSTRAINT PK_CUSTOMERS PRIMARY KEY (CUSTOMER_ID)
            );
            COMMENT ON TABLE ODS.CUSTOMERS IS 'Master customers.';
            COMMENT ON COLUMN ODS.CUSTOMERS.CUSTOMER_ID IS 'PK';
        """)

        code_lines = original.splitlines()

        # Simulate LLM response: fix line 6 (change SYSDATE to CURRENT_TIMESTAMP)
        # and line 5 (change VARCHAR2 to VARCHAR)
        llm_response = json.dumps({
            "edits": [
                {"start_line": 6, "end_line": 6, "new_content": "    CREATED_DATE DATE DEFAULT CURRENT_TIMESTAMP(),"},
                {"start_line": 3, "end_line": 3, "new_content": "    CUSTOMER_NAME VARCHAR(150) NOT NULL,"},
            ]
        })

        # Parse and apply (same logic as self_healing.py)
        edit_data = json.loads(llm_response)
        edits = edit_data["edits"]
        sorted_edits = sorted(edits, key=lambda e: e["start_line"], reverse=True)
        result_lines = code_lines.copy()

        for edit in sorted_edits:
            start = edit["start_line"]
            end = edit["end_line"]
            new_content = edit["new_content"]
            new_lines = new_content.split("\n") if new_content else []
            result_lines[start - 1: end] = new_lines

        fixed_code = "\n".join(result_lines)

        # Verify
        self.assertIn("CURRENT_TIMESTAMP()", fixed_code)
        self.assertIn("VARCHAR(150)", fixed_code)
        self.assertNotIn("SYSDATE", fixed_code)
        self.assertNotIn("VARCHAR2(150)", fixed_code)
        # Unchanged lines preserved
        self.assertIn("CUSTOMER_ID NUMBER(10)", fixed_code)
        self.assertIn("EMAIL_ID VARCHAR2(200)", fixed_code)
        self.assertIn("COMMENT ON TABLE", fixed_code)
        self.assertIn("COMMENT ON COLUMN", fixed_code)
        # Total lines preserved
        self.assertEqual(len(result_lines), 10)


if __name__ == "__main__":
    unittest.main()
