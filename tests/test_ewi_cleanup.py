"""Tests for ewi_cleanup service — validates all EWI marker patterns."""

import os
import shutil
import tempfile
import textwrap
import unittest

from agentic_core.services.ewi_cleanup import clean_ewi_markers, clean_ewi_from_file


class TestCleanEwiMarkers(unittest.TestCase):
    """Tests for clean_ewi_markers()."""

    def test_no_markers_returns_unchanged(self):
        sql = "CREATE TABLE foo (id NUMBER NOT NULL);"
        self.assertEqual(clean_ewi_markers(sql), sql)

    def test_empty_string(self):
        self.assertEqual(clean_ewi_markers(""), "")

    def test_none_like(self):
        self.assertEqual(clean_ewi_markers(""), "")

    # --- Pattern A: Inline marker + CHECK on same line ---

    def test_inline_check_with_comma(self):
        """EWI marker + CHECK on same line, followed by comma."""
        sql = textwrap.dedent("""\
            CREATE TABLE T (
                C1 CHAR(1) DEFAULT 'Y'
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED ***/!!! CHECK (C1 IN ('Y','N')),
                C2 NUMBER(10) NOT NULL
            );""")
        result = clean_ewi_markers(sql)
        self.assertNotIn("!!!RESOLVE EWI!!!", result)
        self.assertNotIn("CHECK", result)
        self.assertIn("CHAR(1) DEFAULT 'Y',", result)
        self.assertIn("C2 NUMBER(10) NOT NULL", result)

    def test_inline_check_without_comma(self):
        """EWI marker + CHECK on same line, no trailing comma (last col)."""
        sql = textwrap.dedent("""\
            CREATE TABLE T (
                C1 CHAR(1) DEFAULT 'Y'
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED ***/!!! CHECK (C1 IN ('Y','N'))
            );""")
        result = clean_ewi_markers(sql)
        self.assertNotIn("!!!RESOLVE EWI!!!", result)
        self.assertNotIn("CHECK", result)
        self.assertIn("CHAR(1) DEFAULT 'Y'", result)
        # Should NOT have trailing comma on last column
        self.assertNotIn("DEFAULT 'Y',", result)

    def test_inline_check_with_prefix_sql(self):
        """SQL content before the marker on the same line."""
        sql = "    ACTIVE CHAR(1) DEFAULT 'Y' !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED ***/!!! CHECK (ACTIVE IN ('Y','N')),"
        result = clean_ewi_markers(sql)
        self.assertNotIn("!!!RESOLVE EWI!!!", result)
        self.assertNotIn("CHECK", result)
        self.assertIn("ACTIVE CHAR(1) DEFAULT 'Y',", result)

    def test_inline_check_no_prefix_comma_propagation(self):
        """CRITICAL: Full line is EWI+CHECK with comma — comma must propagate to previous line."""
        sql = textwrap.dedent("""\
            CREATE TABLE T (
                ACTIVE_FLAG CHAR(1) DEFAULT 'Y'
                                               !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED ***/!!! CHECK (ACTIVE_FLAG IN ('Y','N')),
                CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            );""")
        result = clean_ewi_markers(sql)
        self.assertNotIn("!!!RESOLVE EWI!!!", result)
        self.assertNotIn("CHECK", result)
        # The comma must be preserved on the ACTIVE_FLAG line
        self.assertIn("DEFAULT 'Y',", result)
        self.assertIn("CREATED_DATE TIMESTAMP", result)

    # --- Pattern B: Multi-line marker + CHECK on next line ---

    def test_multiline_check(self):
        """EWI marker on its own line, CHECK on the next."""
        sql = textwrap.dedent("""\
            CREATE TABLE T (
                QTY NUMBER(12,2) DEFAULT 0
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED ***/!!!
                    CHECK (QTY >= 0),
                C2 NUMBER(10) NOT NULL
            );""")
        result = clean_ewi_markers(sql)
        self.assertNotIn("!!!RESOLVE EWI!!!", result)
        self.assertNotIn("CHECK", result)
        # Comma must propagate to the QTY line
        self.assertIn("DEFAULT 0,", result)
        self.assertIn("C2 NUMBER(10) NOT NULL", result)

    def test_multiline_check_no_comma(self):
        """Multi-line EWI + CHECK without trailing comma (last item)."""
        sql = textwrap.dedent("""\
            CREATE TABLE T (
                QTY NUMBER(12,2) DEFAULT 0
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED ***/!!!
                    CHECK (QTY >= 0)
            );""")
        result = clean_ewi_markers(sql)
        self.assertNotIn("CHECK", result)
        self.assertIn("DEFAULT 0", result)
        self.assertNotIn("DEFAULT 0,", result)

    def test_multiline_constraint_check(self):
        """EWI marker line, followed by CONSTRAINT ... CHECK on next line."""
        sql = textwrap.dedent("""\
            CREATE TABLE T (
                C1 NUMBER NOT NULL,
                !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED ***/!!!
                CONSTRAINT CHK_DATES CHECK (END_DATE >= START_DATE)
            );""")
        result = clean_ewi_markers(sql)
        self.assertNotIn("!!!RESOLVE EWI!!!", result)
        self.assertNotIn("CONSTRAINT CHK_DATES", result)
        self.assertNotIn("CHECK", result)
        self.assertIn("C1 NUMBER NOT NULL", result)

    # --- Pattern C: Standalone marker (no CHECK) ---

    def test_standalone_marker_no_check(self):
        """EWI marker with no CHECK clause following."""
        sql = textwrap.dedent("""\
            CREATE TABLE T (
                C1 NUMBER NOT NULL
                !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - SOME OTHER WARNING ***/!!!
            );""")
        result = clean_ewi_markers(sql)
        self.assertNotIn("!!!RESOLVE EWI!!!", result)
        self.assertIn("C1 NUMBER NOT NULL", result)

    # --- Edge cases ---

    def test_multiple_ewi_patterns_in_one_file(self):
        """Mix of inline, multi-line, and standalone patterns."""
        sql = textwrap.dedent("""\
            CREATE TABLE T (
                C1 CHAR(1) DEFAULT 'Y'
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED ***/!!! CHECK (C1 IN ('Y','N')),
                C2 NUMBER(12,2) DEFAULT 0
                    !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED ***/!!!
                    CHECK (C2 >= 0),
                C3 NUMBER NOT NULL,
                !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED ***/!!!
                CONSTRAINT CHK_C3 CHECK (C3 > 0)
            );""")
        result = clean_ewi_markers(sql)
        self.assertNotIn("!!!RESOLVE EWI!!!", result)
        self.assertNotIn("CHECK", result)
        self.assertIn("C1 CHAR(1) DEFAULT 'Y',", result)
        self.assertIn("C2 NUMBER(12,2) DEFAULT 0,", result)
        self.assertIn("C3 NUMBER NOT NULL", result)

    def test_nested_parentheses_in_check(self):
        """CHECK clause with nested parens."""
        sql = "    !!!RESOLVE EWI!!! /*** SSC-EWI-0035 ***/!!! CHECK (CASE WHEN (X > 0) THEN 'Y' ELSE 'N' END = 'Y'),"
        result = clean_ewi_markers(sql)
        self.assertNotIn("CHECK", result)
        self.assertNotIn("!!!RESOLVE EWI!!!", result)

    def test_preserves_non_ewi_comments(self):
        """Regular SQL comments should not be affected."""
        sql = textwrap.dedent("""\
            -- This is a regular comment
            /*** SSC-FDM-0006 - NUMBER TYPE COLUMN MAY NOT BEHAVE SIMILARLY IN SNOWFLAKE. ***/
            CREATE TABLE T (ID NUMBER);""")
        result = clean_ewi_markers(sql)
        self.assertIn("-- This is a regular comment", result)
        self.assertIn("SSC-FDM-0006", result)

    def test_already_commented_ewi_untouched(self):
        """EWI markers that were already converted to comments by the LLM."""
        sql = "    /* RESOLVE EWI - SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED */ /* CHECK (C1 IN ('Y','N')) */,"
        result = clean_ewi_markers(sql)
        # These are already valid SQL comments, should remain unchanged
        self.assertIn("/* RESOLVE EWI", result)

    def test_real_world_snowflake_failure(self):
        """The exact pattern that caused the Snowflake syntax error."""
        sql = textwrap.dedent("""\
            CREATE OR REPLACE TABLE DBMIG_POC.DBMIG_POC.STORES (
                STORE_ID NUMBER(10) NOT NULL,
                STORE_CODE VARCHAR(30) NOT NULL,
                STATUS_FLAG           CHAR(1)              DEFAULT 'O'
                                                                       !!!RESOLVE EWI!!! /*** SSC-EWI-0035 - CHECK STATEMENT NOT SUPPORTED ***/!!! CHECK (STATUS_FLAG IN ('O','C')),
                CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_TS            TIMESTAMP(6),
                CONSTRAINT PK_STORES PRIMARY KEY (STORE_ID)
            );""")
        result = clean_ewi_markers(sql)
        self.assertNotIn("!!!RESOLVE EWI!!!", result)
        self.assertNotIn("CHECK", result)
        # Critical: comma must be preserved for valid SQL
        self.assertIn("DEFAULT 'O',", result)
        self.assertIn("CREATED_DATE TIMESTAMP", result)


class TestCleanEwiFromFile(unittest.TestCase):
    """Tests for clean_ewi_from_file()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_cleans_file_in_place(self):
        filepath = os.path.join(self.tmpdir, "test.sql")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("C1 CHAR(1) !!!RESOLVE EWI!!! /*** SSC-EWI-0035 ***/!!! CHECK (C1 IN ('Y','N')),\nC2 NUMBER;")

        changed = clean_ewi_from_file(filepath)
        self.assertTrue(changed)

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertNotIn("!!!RESOLVE EWI!!!", content)
        self.assertNotIn("CHECK", content)

    def test_no_change_returns_false(self):
        filepath = os.path.join(self.tmpdir, "clean.sql")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("SELECT 1;")

        changed = clean_ewi_from_file(filepath)
        self.assertFalse(changed)

    def test_missing_file(self):
        changed = clean_ewi_from_file("/nonexistent.sql")
        self.assertFalse(changed)


class TestRealWorldFile(unittest.TestCase):
    """Test against the actual partially_fixed_output.sql if available."""

    REAL_FILE = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "sql_scripts", "partially_fixed_output.sql"
    )

    @unittest.skipUnless(
        os.path.exists(REAL_FILE),
        "partially_fixed_output.sql not found"
    )
    def test_real_file_cleanup(self):
        """Verify all EWI markers are removed from the real file."""
        with open(self.REAL_FILE, "r", encoding="utf-8") as f:
            original = f.read()

        cleaned = clean_ewi_markers(original)

        # No EWI markers should remain
        self.assertNotIn("!!!RESOLVE EWI!!!", cleaned)

        # The SQL structure should still be intact
        self.assertIn("CREATE TABLE", cleaned)
        self.assertIn("COMMENT ON TABLE", cleaned)
        self.assertIn("CONSTRAINT PK_CUSTOMERS PRIMARY KEY", cleaned)

        # Count that substantial content remains (no truncation)
        line_count = len(cleaned.splitlines())
        self.assertGreater(line_count, 500, f"Only {line_count} lines — possible truncation")


if __name__ == "__main__":
    unittest.main()

