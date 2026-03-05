from __future__ import annotations

import unittest
from unittest.mock import patch

from python_execution_service import main


def _make_run() -> main.RunRecord:
    return main.RunRecord(
        runId="run-1",
        projectId="proj-1",
        projectName="project",
        sourceId="src-1",
        schemaId="schema-1",
        sourceLanguage="teradata",
        sourcePath="source.sql",
        schemaPath="mapping.csv",
        sfAccount=None,
        sfUser=None,
        sfRole=None,
        sfWarehouse=None,
        sfDatabase=None,
        sfSchema=None,
        sfAuthenticator=None,
        status="running",
        createdAt=main.now_iso(),
        updatedAt=main.now_iso(),
        outputDir=".",
    )


class TerminalEventTests(unittest.TestCase):
    def test_append_terminal_event_appends_and_emits(self) -> None:
        run = _make_run()
        emitted: list[tuple[str, dict]] = []

        with patch.object(main, "persist_runs_locked", return_value=None):
            with patch.object(
                main,
                "append_event",
                side_effect=lambda _run, event_type, payload: emitted.append((event_type, payload)),
            ):
                command = main.append_terminal_event(
                    run,
                    event_type="terminal:command",
                    step_id="convert_code",
                    command="scai code convert",
                    cwd="projects/demo",
                    attempt=1,
                )
                line = main.append_terminal_event(
                    run,
                    event_type="terminal:line",
                    step_id="convert_code",
                    stream="stderr",
                    text="Compilation failed",
                )

        self.assertEqual(command["type"], "terminal:command")
        self.assertEqual(line["type"], "terminal:line")
        self.assertEqual(line["stream"], "stderr")
        self.assertEqual(len(run.terminalEvents), 2)
        self.assertEqual(len(run.messages), 0)
        self.assertEqual(emitted[0][0], "terminal:command")
        self.assertEqual(emitted[1][0], "terminal:line")

    def test_route_activity_terminal_entry_skips_chat_logs(self) -> None:
        run = _make_run()
        entry = {
            "timestamp": "2026-03-05T10:00:00",
            "stage": "convert_code",
            "data": {
                "terminal": {
                    "type": "line",
                    "stepId": "convert_code",
                    "stream": "stdout",
                    "text": "line from CLI",
                }
            },
        }

        with patch.object(main, "append_terminal_event", return_value={}) as append_terminal:
            with patch.object(main, "add_log", return_value=None) as add_log:
                main.route_activity_log_entry(run, entry)

        append_terminal.assert_called_once()
        add_log.assert_not_called()

    def test_route_activity_non_terminal_entry_uses_chat_logs(self) -> None:
        run = _make_run()
        entry = {
            "timestamp": "2026-03-05T10:00:00",
            "stage": "convert_code",
            "message": "Schema mapping complete",
        }

        with patch.object(main, "append_terminal_event", return_value={}) as append_terminal:
            with patch.object(main, "add_log", return_value=None) as add_log:
                main.route_activity_log_entry(run, entry)

        append_terminal.assert_not_called()
        add_log.assert_called_once()


if __name__ == "__main__":
    unittest.main()

