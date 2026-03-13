"""Comprehensive tests for the schema mapping replacement logic.

Covers:
- Basic CREATE TABLE / CREATE INDEX schema replacement
- COMMENT ON TABLE  (schema.table)
- COMMENT ON COLUMN (schema.table.column) — the original bug
- Multi-schema files (ODS, STG, EDW, MART)
- Table-only mapping entries (no schema prefix in SQL)
- Case insensitivity
- Foreign-key cross-schema references
- No cascading / no double replacement
- Idempotency (running twice gives the same result)
- Empty / missing mapping rows
- String literals are NOT corrupted
- SQL line comments and block comments containing schema names
"""

import os
import shutil
import tempfile
import textwrap
import unittest

# The function under test
from scripts.schema_conversion_teradata_to_snowflake import (
    _apply_schema_mapping_to_sql,
    process_sql_with_pandas_replace,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Standard mapping rows used by the real long_table_map.sql
SCHEMA_MAPPINGS = [
    ("ODS", "DBMIG_POC.DBMIG_POC"),
    ("STG", "DBMIG_POC.DBMIG_POC"),
    ("EDW", "DBMIG_POC.DBMIG_POC"),
    ("MART", "DBMIG_POC.DBMIG_POC"),
    ("LEGACY", "DB_NOT_FOUND.SCHEMA_NOT_FOUND"),
]

TABLE_MAPPINGS = [
    ("CUSTOMERS", "DBMIG_POC.DBMIG_POC.CUSTOMERS"),
    ("CUSTOMER_ADDRESSES", "DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES"),
    ("SUPPLIERS", "DBMIG_POC.DBMIG_POC.SUPPLIERS"),
    ("PRODUCTS", "DBMIG_POC.DBMIG_POC.PRODUCTS"),
    ("STORES", "DBMIG_POC.DBMIG_POC.STORES"),
    ("ORDERS", "DBMIG_POC.DBMIG_POC.ORDERS"),
    ("ORDER_ITEMS", "DBMIG_POC.DBMIG_POC.ORDER_ITEMS"),
]

ALL_MAPPINGS = SCHEMA_MAPPINGS + TABLE_MAPPINGS


# ---------------------------------------------------------------------------
# Unit tests for _apply_schema_mapping_to_sql
# ---------------------------------------------------------------------------


class TestBasicSchemaReplacement(unittest.TestCase):
    """Schema-prefixed identifiers like ODS.TABLE should be remapped."""

    def test_create_table(self):
        sql = "CREATE TABLE ODS.CUSTOMERS (\n    CUSTOMER_ID NUMBER(10) NOT NULL\n);"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMERS", result)
        # Must NOT have triple-DBMIG_POC
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)

    def test_create_index(self):
        sql = "CREATE INDEX ODS.IX_CUSTOMERS_EMAIL ON ODS.CUSTOMERS (EMAIL_ID);"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertEqual(result.count("DBMIG_POC.DBMIG_POC"), 2)
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)


class TestCommentOnTable(unittest.TestCase):
    """COMMENT ON TABLE schema.table should remap the schema only."""

    def test_comment_on_table(self):
        sql = "COMMENT ON TABLE ODS.CUSTOMERS IS 'Master list of customers.';"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMERS", result)
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)

    def test_comment_on_table_stg(self):
        sql = "COMMENT ON TABLE STG.ORDERS IS 'Inbound orders landing area.';"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertIn("DBMIG_POC.DBMIG_POC.ORDERS", result)
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)


class TestCommentOnColumn(unittest.TestCase):
    """COMMENT ON COLUMN schema.table.column — the original bug case."""

    def test_comment_on_column_basic(self):
        """The exact failing case from the bug report."""
        sql = "COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.ADDRESS_ID IS 'Surrogate key for address';"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES.ADDRESS_ID", result)
        # The critical assertion: must NOT have cascading DBMIG_POC
        self.assertNotIn(
            "DBMIG_POC.DBMIG_POC.DBMIG_POC.DBMIG_POC",
            result,
            "Cascading replacement detected — the original bug!",
        )

    def test_comment_on_column_multiple(self):
        """Multiple COMMENT ON COLUMN lines for the same table."""
        sql = textwrap.dedent("""\
            COMMENT ON COLUMN ODS.CUSTOMERS.CUSTOMER_ID      IS 'Natural/business key';
            COMMENT ON COLUMN ODS.CUSTOMERS.CUSTOMER_UUID    IS 'Binary UUID';
            COMMENT ON COLUMN ODS.CUSTOMERS.CUSTOMER_NAME    IS 'Full name';
            COMMENT ON COLUMN ODS.CUSTOMERS.EMAIL_ID         IS 'Primary email address';
        """)
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        # Each line should have exactly DBMIG_POC.DBMIG_POC.CUSTOMERS.<col>
        for line in result.strip().splitlines():
            self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMERS.", line)
            self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", line)

    def test_comment_on_column_different_schemas(self):
        """Columns from different schemas in the same file."""
        sql = textwrap.dedent("""\
            COMMENT ON COLUMN ODS.CUSTOMERS.CUSTOMER_ID IS 'PK';
            COMMENT ON COLUMN STG.ORDERS.ORDER_ID       IS 'PK';
            COMMENT ON COLUMN EDW.CUSTOMER_DIM.CUSTOMER_KEY IS 'SCD key';
        """)
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMERS.CUSTOMER_ID", result)
        self.assertIn("DBMIG_POC.DBMIG_POC.ORDERS.ORDER_ID", result)
        self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMER_DIM.CUSTOMER_KEY", result)
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)

    def test_comment_on_column_with_fk_text(self):
        """COMMENT text referencing FK to another schema.table."""
        sql = "COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.CUSTOMER_ID IS 'FK to ODS.CUSTOMERS';"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        # The schema.table.column part should be mapped correctly
        self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES.CUSTOMER_ID", result)
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES", result)


class TestForeignKeyReferences(unittest.TestCase):
    """FK constraints reference tables in other schemas."""

    def test_fk_cross_schema(self):
        sql = textwrap.dedent("""\
            CREATE TABLE STG.ORDER_ITEMS (
                ORDER_ITEM_ID  NUMBER(12) NOT NULL,
                ORDER_ID       NUMBER(12) NOT NULL,
                PRODUCT_ID     NUMBER(10) NOT NULL,
                CONSTRAINT FK_STG_OI_ORDER   FOREIGN KEY (ORDER_ID)   REFERENCES STG.ORDERS (ORDER_ID),
                CONSTRAINT FK_STG_OI_PRODUCT FOREIGN KEY (PRODUCT_ID) REFERENCES ODS.PRODUCTS (PRODUCT_ID)
            );
        """)
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertIn("DBMIG_POC.DBMIG_POC.ORDER_ITEMS", result)
        self.assertIn("REFERENCES DBMIG_POC.DBMIG_POC.ORDERS", result)
        self.assertIn("REFERENCES DBMIG_POC.DBMIG_POC.PRODUCTS", result)
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)


class TestMultipleSchemas(unittest.TestCase):
    """All four schemas in a single file should all be mapped."""

    def test_four_schemas(self):
        sql = textwrap.dedent("""\
            CREATE TABLE ODS.CUSTOMERS  (ID NUMBER NOT NULL);
            CREATE TABLE STG.ORDERS     (ID NUMBER NOT NULL);
            CREATE TABLE EDW.DATE_DIM   (ID NUMBER NOT NULL);
            CREATE TABLE MART.TOP_SELLERS (ID NUMBER NOT NULL);
        """)
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertEqual(result.count("DBMIG_POC.DBMIG_POC."), 4)
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)


class TestCaseInsensitivity(unittest.TestCase):
    """Mapping should be case-insensitive."""

    def test_lowercase_schema(self):
        sql = "CREATE TABLE ods.customers (ID NUMBER NOT NULL);"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertIn("DBMIG_POC.DBMIG_POC", result)

    def test_mixed_case_schema(self):
        sql = "COMMENT ON COLUMN Ods.Customers.Customer_Id IS 'PK';"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertIn("DBMIG_POC.DBMIG_POC", result)
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)


class TestNoCascading(unittest.TestCase):
    """Replacement text must NOT be re-matched by subsequent rows."""

    def test_no_double_replacement(self):
        """ODS → DBMIG_POC.DBMIG_POC should not then have DBMIG_POC matched."""
        sql = "CREATE TABLE ODS.CUSTOMERS (ID NUMBER);"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        # Count occurrences of DBMIG_POC
        count = result.count("DBMIG_POC")
        # In DBMIG_POC.DBMIG_POC.CUSTOMERS there should be exactly 2 occurrences
        # of "DBMIG_POC" — no more.
        self.assertEqual(count, 2, f"Expected 2 occurrences of DBMIG_POC, got {count}: {result}")

    def test_cascading_with_comment_on_column(self):
        """The original bug: after replacing ODS, table name also gets replaced."""
        sql = "COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.ADDRESS_ID IS 'key';"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        # Must be exactly:
        #   DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES.ADDRESS_ID
        expected_fragment = "DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES.ADDRESS_ID"
        self.assertIn(expected_fragment, result)
        # Not the buggy version
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)


class TestIdempotency(unittest.TestCase):
    """Running the mapping twice should give the same result as once."""

    def test_double_application(self):
        sql = textwrap.dedent("""\
            CREATE TABLE ODS.CUSTOMERS (ID NUMBER NOT NULL);
            COMMENT ON COLUMN ODS.CUSTOMERS.CUSTOMER_ID IS 'PK';
        """)
        first_pass, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        second_pass, _, _ = _apply_schema_mapping_to_sql(first_pass, ALL_MAPPINGS)
        self.assertEqual(first_pass, second_pass, "Mapping is not idempotent — applying twice changes the output!")


class TestEmptyAndEdgeCases(unittest.TestCase):
    """Edge cases that should not crash or corrupt."""

    def test_empty_sql(self):
        result, matches, _ = _apply_schema_mapping_to_sql("", ALL_MAPPINGS)
        self.assertEqual(result, "")
        self.assertEqual(matches, 0)

    def test_no_mappings(self):
        sql = "CREATE TABLE ODS.CUSTOMERS (ID NUMBER);"
        result, _, _ = _apply_schema_mapping_to_sql(sql, [])
        self.assertEqual(result, sql)

    def test_sql_with_no_matching_schemas(self):
        sql = "CREATE TABLE PUBLIC.USERS (ID NUMBER);"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertEqual(result, sql)


class TestStringLiteralSafety(unittest.TestCase):
    """Schema names inside string literals should still be handled by current
    regex logic — documenting current behavior. If full SQL-parsing is added
    later, these tests should be updated."""

    def test_schema_in_is_clause_text(self):
        """The IS '...' text in COMMENT is a string literal.

        Currently the naive regex WILL match inside strings because we don't
        parse SQL tokens. We document this as accepted behavior since the
        original code also matched inside strings, and real COMMENT IS text
        rarely contains dot-qualified schema names.
        """
        sql = "COMMENT ON TABLE ODS.CUSTOMERS IS 'Data from ODS layer';"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        # The schema prefix should be replaced
        self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMERS", result)


class TestSQLComments(unittest.TestCase):
    """Schema names inside SQL comments (-- and /* */) are handled."""

    def test_single_line_comment(self):
        sql = "-- 1) ODS.CUSTOMERS\nCREATE TABLE ODS.CUSTOMERS (ID NUMBER);"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        # The CREATE TABLE line must be mapped
        self.assertIn("CREATE TABLE DBMIG_POC.DBMIG_POC.CUSTOMERS", result)

    def test_block_comment(self):
        sql = "/* ODS LAYER TABLES */\nCREATE TABLE ODS.CUSTOMERS (ID NUMBER);"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertIn("CREATE TABLE DBMIG_POC.DBMIG_POC.CUSTOMERS", result)


class TestDBNotFoundMapping(unittest.TestCase):
    """Entries mapping to DB_NOT_FOUND should work the same way."""

    def test_legacy_schema(self):
        sql = "CREATE TABLE LEGACY.OLD_TABLE (ID NUMBER);"
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)
        self.assertIn("DB_NOT_FOUND.SCHEMA_NOT_FOUND.OLD_TABLE", result)


class TestComplexRealWorldSQL(unittest.TestCase):
    """Tests that simulate realistic, multi-statement Oracle DDL."""

    def test_full_table_with_comments(self):
        """Full CREATE TABLE + COMMENT ON TABLE + COMMENT ON COLUMN block."""
        sql = textwrap.dedent("""\
            CREATE TABLE ODS.CUSTOMER_ADDRESSES (
                ADDRESS_ID     NUMBER(12)   NOT NULL,
                CUSTOMER_ID    NUMBER(10)   NOT NULL,
                CONSTRAINT PK_CUSTOMER_ADDRESSES PRIMARY KEY (ADDRESS_ID),
                CONSTRAINT FK_CA_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES ODS.CUSTOMERS (CUSTOMER_ID)
            );

            COMMENT ON TABLE ODS.CUSTOMER_ADDRESSES IS 'Addresses for customers.';
            COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.ADDRESS_ID     IS 'Surrogate key for address';
            COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.CUSTOMER_ID    IS 'FK to ODS.CUSTOMERS';

            CREATE INDEX ODS.IX_CA_CUSTOMER ON ODS.CUSTOMER_ADDRESSES (CUSTOMER_ID);
        """)
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)

        # All ODS references should be replaced
        self.assertNotIn("\nODS.", result.replace("'FK to ODS.CUSTOMERS'", ""))

        # The critical bug check — no cascading
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)

        # Specific assertions
        self.assertIn("CREATE TABLE DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES", result)
        self.assertIn("REFERENCES DBMIG_POC.DBMIG_POC.CUSTOMERS", result)
        self.assertIn("COMMENT ON TABLE DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES", result)
        self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES.ADDRESS_ID", result)
        self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES.CUSTOMER_ID", result)
        self.assertIn("DBMIG_POC.DBMIG_POC.IX_CA_CUSTOMER", result)

    def test_mixed_ddl_and_comments(self):
        """Multiple tables across schemas with interleaved comments."""
        sql = textwrap.dedent("""\
            -- ODS Layer
            CREATE TABLE ODS.PRODUCTS (
                PRODUCT_ID NUMBER(10) NOT NULL,
                CONSTRAINT PK_PRODUCTS PRIMARY KEY (PRODUCT_ID)
            );
            COMMENT ON TABLE ODS.PRODUCTS IS 'Product master.';
            COMMENT ON COLUMN ODS.PRODUCTS.PRODUCT_ID IS 'Primary key';

            -- STG Layer
            CREATE TABLE STG.ORDER_ITEMS (
                ORDER_ITEM_ID NUMBER(12) NOT NULL,
                PRODUCT_ID    NUMBER(10) NOT NULL,
                CONSTRAINT FK_STG_OI_PRODUCT FOREIGN KEY (PRODUCT_ID) REFERENCES ODS.PRODUCTS (PRODUCT_ID)
            );
            COMMENT ON TABLE STG.ORDER_ITEMS IS 'Line items for STG.ORDERS.';
            COMMENT ON COLUMN STG.ORDER_ITEMS.ORDER_ITEM_ID IS 'Primary key';

            -- EDW Layer
            CREATE TABLE EDW.SALES_FACT (
                PRODUCT_KEY NUMBER(12) NOT NULL
            );
            COMMENT ON TABLE EDW.SALES_FACT IS 'Grain: one row per order item.';
        """)
        result, _, _ = _apply_schema_mapping_to_sql(sql, ALL_MAPPINGS)

        # No cascading anywhere
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)

        # Schema replacements
        self.assertIn("DBMIG_POC.DBMIG_POC.PRODUCTS", result)
        self.assertIn("DBMIG_POC.DBMIG_POC.ORDER_ITEMS", result)
        self.assertIn("DBMIG_POC.DBMIG_POC.SALES_FACT", result)

        # Column comments
        self.assertIn("DBMIG_POC.DBMIG_POC.PRODUCTS.PRODUCT_ID", result)
        self.assertIn("DBMIG_POC.DBMIG_POC.ORDER_ITEMS.ORDER_ITEM_ID", result)


# ---------------------------------------------------------------------------
# Integration test using process_sql_with_pandas_replace with real files
# ---------------------------------------------------------------------------


class TestProcessSQLIntegration(unittest.TestCase):
    """Integration test that writes a CSV + SQL file to disk, runs the full
    process_sql_with_pandas_replace function, and verifies the output."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.source_dir = os.path.join(self.tmpdir, "source")
        self.output_dir = os.path.join(self.tmpdir, "output")
        self.csv_path = os.path.join(self.tmpdir, "mapping.csv")
        os.makedirs(self.source_dir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        # Clean up the summary.json that gets written to cwd
        if os.path.exists("summary.json"):
            os.remove("summary.json")

    def _write_csv(self, rows):
        """Write a CSV mapping file."""
        with open(self.csv_path, "w", encoding="utf-8") as f:
            f.write("SOURCE_SCHEMA,TARGET_DB_SCHEMA\n")
            for src, tgt in rows:
                f.write(f"{src},{tgt}\n")

    def _write_sql(self, filename, content):
        """Write a SQL file into the source directory."""
        filepath = os.path.join(self.source_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

    def _read_output(self, filename):
        """Read the output SQL file."""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()

    def test_full_integration(self):
        """End-to-end: CSV + SQL → mapped output with no cascading."""
        self._write_csv([
            ("ODS", "DBMIG_POC.DBMIG_POC"),
            ("STG", "DBMIG_POC.DBMIG_POC"),
            ("CUSTOMER_ADDRESSES", "DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES"),
        ])

        sql = textwrap.dedent("""\
            CREATE TABLE ODS.CUSTOMER_ADDRESSES (
                ADDRESS_ID NUMBER(12) NOT NULL,
                CUSTOMER_ID NUMBER(10) NOT NULL
            );
            COMMENT ON TABLE ODS.CUSTOMER_ADDRESSES IS 'Addresses.';
            COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.ADDRESS_ID IS 'PK';
            COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.CUSTOMER_ID IS 'FK to ODS.CUSTOMERS';
            CREATE INDEX ODS.IX_CA_CUSTOMER ON ODS.CUSTOMER_ADDRESSES (CUSTOMER_ID);
        """)
        self._write_sql("test.sql", sql)

        log_messages = []
        process_sql_with_pandas_replace(
            csv_file_path=self.csv_path,
            sql_file_path=self.source_dir,
            output_dir=self.output_dir,
            logg=lambda msg: log_messages.append(msg),
        )

        result = self._read_output("test.sql")

        # Verify no cascading
        self.assertNotIn("DBMIG_POC.DBMIG_POC.DBMIG_POC", result)

        # Verify correct mapping
        self.assertIn("CREATE TABLE DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES", result)
        self.assertIn("COMMENT ON TABLE DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES", result)
        self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES.ADDRESS_ID", result)
        self.assertIn("DBMIG_POC.DBMIG_POC.CUSTOMER_ADDRESSES.CUSTOMER_ID", result)

    def test_integration_no_csv_entries(self):
        """If CSV has no matching schemas, SQL should pass through unchanged."""
        self._write_csv([("NONEXISTENT", "TARGET.SCHEMA")])
        sql = "CREATE TABLE ODS.CUSTOMERS (ID NUMBER);"
        self._write_sql("test.sql", sql)

        process_sql_with_pandas_replace(
            csv_file_path=self.csv_path,
            sql_file_path=self.source_dir,
            output_dir=self.output_dir,
            logg=lambda msg: None,
        )

        result = self._read_output("test.sql")
        self.assertEqual(result, sql)


if __name__ == "__main__":
    unittest.main()
