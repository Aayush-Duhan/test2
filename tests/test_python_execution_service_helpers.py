import tempfile
import unittest
from unittest.mock import patch

from python_execution_service.helpers import add_log
from python_execution_service.models import RunRecord


class PythonExecutionServiceHelpersTests(unittest.TestCase):
    def _make_run(self, output_dir: str) -> RunRecord:
        return RunRecord(
            runId="run-1",
            projectId="project-1",
            projectName="Project 1",
            sourceId="source-1",
            schemaId="schema-1",
            sourceLanguage="teradata",
            sourcePath="source.sql",
            schemaPath="schema.csv",
            sfAccount=None,
            sfUser=None,
            sfRole=None,
            sfWarehouse=None,
            sfDatabase=None,
            sfSchema=None,
            sfAuthenticator=None,
            status="running",
            createdAt="2026-03-08T00:00:00",
            updatedAt="2026-03-08T00:00:00",
            outputDir=output_dir,
        )

    @patch("python_execution_service.helpers.sqlite_store.append_run_log")
    @patch("python_execution_service.helpers.persist_runs_locked")
    def test_add_log_emits_terminal_progress_part(
        self,
        persist_runs_locked_mock,
        append_run_log_mock,
    ):
        with tempfile.TemporaryDirectory() as tmp_dir:
            run = self._make_run(tmp_dir)

            add_log(run, "Converting source")

            self.assertEqual(run.logs, ["Converting source"])
            self.assertEqual(run.messages, [])
            self.assertEqual(len(run.streamParts), 1)
            self.assertEqual(run.streamParts[0]["type"], "data-terminal-progress")
            self.assertEqual(run.streamParts[0]["data"]["text"], "Converting source")
            self.assertFalse(run.streamParts[0]["data"]["isProgress"])
            append_run_log_mock.assert_called_once()
            persist_runs_locked_mock.assert_called()

    @patch("python_execution_service.helpers.persist_runs_locked")
    def test_add_log_progress_emits_progress_terminal_part(
        self,
        persist_runs_locked_mock,
    ):
        with tempfile.TemporaryDirectory() as tmp_dir:
            run = self._make_run(tmp_dir)

            add_log(run, "42% complete", is_progress=True)

            self.assertEqual(run.logs, ["42% complete"])
            self.assertEqual(run.messages, [])
            self.assertEqual(len(run.streamParts), 1)
            self.assertEqual(run.streamParts[0]["type"], "data-terminal-progress")
            self.assertEqual(run.streamParts[0]["data"]["text"], "42% complete")
            self.assertTrue(run.streamParts[0]["data"]["isProgress"])
            persist_runs_locked_mock.assert_called()
