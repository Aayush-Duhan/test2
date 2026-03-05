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


class ChatMessageSchemaTests(unittest.TestCase):
    def test_sanitize_content_removes_tags_and_ansi(self) -> None:
        raw = "[12:34] \u001b[31mhello\u001b[0m\n====="
        self.assertEqual(main._sanitize_content(raw), "hello")

    def test_append_chat_message_persists_schema_fields(self) -> None:
        run = _make_run()
        recorded_events: list[tuple[str, dict]] = []

        with patch.object(main, "persist_runs_locked", return_value=None):
            with patch.object(main.sqlite_store, "append_run_message", return_value=None):
                with patch.object(
                    main,
                    "append_event",
                    side_effect=lambda _run, event_type, payload: recorded_events.append((event_type, payload)),
                ):
                    msg = main.append_chat_message(
                        run,
                        role="agent",
                        kind="log",
                        content="[12:34] message body",
                        step={"id": "convert_code", "label": "Convert SQL"},
                    )

        self.assertEqual(msg["role"], "agent")
        self.assertEqual(msg["kind"], "log")
        self.assertEqual(msg["content"], "message body")
        self.assertEqual(msg["step"], {"id": "convert_code", "label": "Convert SQL"})
        self.assertEqual(run.messages[-1]["id"], msg["id"])
        self.assertEqual(recorded_events[-1][0], "chat:message")

    def test_append_chat_message_keeps_sql_payload(self) -> None:
        run = _make_run()
        with patch.object(main, "persist_runs_locked", return_value=None):
            with patch.object(main.sqlite_store, "append_run_message", return_value=None):
                with patch.object(main, "append_event", return_value=None):
                    msg = main.append_chat_message(
                        run,
                        role="error",
                        kind="sql_error",
                        content="Stmt 2 ERROR",
                        sql={
                            "failedStatement": "select * from missing_table;",
                            "error": "SQL compilation error",
                            "output": "Error type: missing_object",
                        },
                    )

        self.assertEqual(msg["kind"], "sql_error")
        self.assertEqual(msg["sql"]["failedStatement"], "select * from missing_table;")
        self.assertEqual(msg["sql"]["error"], "SQL compilation error")


if __name__ == "__main__":
    unittest.main()
