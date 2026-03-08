import unittest
from unittest.mock import patch

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.models.results import SelfHealResult, ValidationResult
from agentic_core.nodes.convert_code import convert_code_node
from agentic_core.nodes.execute_sql import execute_sql_node
from agentic_core.nodes.self_heal import self_heal_node
from agentic_core.nodes.validate import validate_node


class NodeTests(unittest.TestCase):
    @patch("agentic_core.nodes.convert_code.build_report_context_memory")
    @patch("agentic_core.nodes.convert_code.read_sql_files")
    @patch("agentic_core.nodes.convert_code.list_sql_files")
    @patch("agentic_core.nodes.convert_code.run_scai_command")
    def test_convert_code_node_success(
        self,
        mock_run_scai_command,
        mock_list_sql_files,
        mock_read_sql_files,
        mock_build_report_context_memory,
    ):
        mock_run_scai_command.return_value = (0, "ok", "")
        mock_list_sql_files.return_value = ["converted/a.sql"]
        mock_read_sql_files.return_value = "SELECT 1;"
        mock_build_report_context_memory.return_value = {
            "ignored_codes": ["X1"],
            "report_scan_summary": {"total_report_issues": 0},
        }

        state = MigrationContext(project_name="demo", project_path="projects/demo")
        updated = convert_code_node(state)

        self.assertTrue(updated.scai_converted)
        self.assertEqual(updated.current_stage, MigrationState.CONVERT_CODE)
        self.assertEqual(updated.converted_files, ["converted/a.sql"])

    @patch("agentic_core.nodes.execute_sql.classify_snowflake_error")
    @patch("agentic_core.nodes.execute_sql.execute_sql_statements")
    @patch("agentic_core.nodes.execute_sql.build_snowflake_connection")
    def test_execute_sql_node_routes_missing_objects_to_human_review(
        self,
        mock_build_connection,
        mock_execute_sql_statements,
        mock_classify_snowflake_error,
    ):
        class DummyConnection:
            def close(self):
                return None

        mock_build_connection.return_value = DummyConnection()
        mock_execute_sql_statements.side_effect = Exception("missing table")
        mock_classify_snowflake_error.return_value = ("missing_object", "DB.SCHEMA.TABLE_X")

        state = MigrationContext(
            project_path="project",
            converted_code="SELECT 1;",
        )
        updated = execute_sql_node(state)

        self.assertEqual(updated.current_stage, MigrationState.HUMAN_REVIEW)
        self.assertTrue(updated.requires_ddl_upload)
        self.assertIn("DB.SCHEMA.TABLE_X", updated.missing_objects)

    @patch("agentic_core.nodes.self_heal.build_report_context_memory")
    @patch("agentic_core.nodes.self_heal.apply_self_healing")
    def test_self_heal_node_updates_state_on_success(
        self,
        mock_apply_self_healing,
        mock_build_report_context_memory,
    ):
        mock_build_report_context_memory.return_value = {
            "ignored_codes": [],
            "report_scan_summary": {},
        }
        mock_apply_self_healing.return_value = SelfHealResult(
            success=True,
            fixed_code="SELECT 2;",
            fixes_applied=["fixed"],
            issues_fixed=1,
            iteration=1,
        )

        state = MigrationContext(converted_code="SELECT 1;", validation_issues=[{"message": "bad"}])
        updated = self_heal_node(state)

        self.assertEqual(updated.converted_code, "SELECT 2;")
        self.assertEqual(updated.current_stage, MigrationState.SELF_HEAL)
        self.assertEqual(updated.self_heal_iteration, 1)
        self.assertEqual(len(updated.self_heal_log), 1)

    @patch("agentic_core.nodes.validate.validate_code")
    def test_validate_node_updates_state_for_pass_and_fail(self, mock_validate_code):
        state = MigrationContext(converted_code="SELECT 1;", original_code="SELECT 1;")

        mock_validate_code.return_value = ValidationResult(
            passed=True,
            issues=[],
            results={"line_count_validation": {"passed": True}},
        )
        updated = validate_node(state)
        self.assertTrue(updated.validation_passed)
        self.assertEqual(updated.final_code, "SELECT 1;")

        failing_state = MigrationContext(converted_code="SELECT 1;", original_code="SELECT 1;")
        mock_validate_code.return_value = ValidationResult(
            passed=False,
            issues=[{"type": "line_count_regression", "message": "bad"}],
            results={"line_count_validation": {"passed": False}},
        )
        updated_fail = validate_node(failing_state)
        self.assertFalse(updated_fail.validation_passed)
        self.assertEqual(updated_fail.validation_issues[0]["type"], "line_count_regression")
