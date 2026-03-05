from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

from agentic_core import nodes


class _FakeStream:
    def __init__(self, lines: list[bytes]) -> None:
        self._lines = list(lines)

    def readline(self) -> bytes:
        if not self._lines:
            return b""
        return self._lines.pop(0)

    def close(self) -> None:
        return None


class _FakeProcess:
    def __init__(self, returncode: int, stdout_lines: list[bytes], stderr_lines: list[bytes]) -> None:
        self._returncode = returncode
        self.stdout = _FakeStream(stdout_lines)
        self.stderr = _FakeStream(stderr_lines)

    def wait(self) -> int:
        return self._returncode


class TerminalStreamingTests(unittest.TestCase):
    def test_run_scai_command_streams_lines_and_captures_output(self) -> None:
        command_events: list[dict] = []
        line_events: list[tuple[str, str]] = []

        code = (
            "import sys,time;"
            "print('out1');sys.stdout.flush();"
            "time.sleep(0.05);"
            "print('err1', file=sys.stderr);sys.stderr.flush();"
            "time.sleep(0.05);"
            "print('out2');sys.stdout.flush()"
        )
        return_code, stdout, stderr = nodes._run_scai_command(
            [sys.executable, "-c", code],
            cwd=os.getcwd(),
            max_retries=1,
            on_command=lambda payload: command_events.append(payload),
            on_line=lambda stream, text: line_events.append((stream, text)),
        )

        self.assertEqual(return_code, 0)
        self.assertEqual(len(command_events), 1)
        self.assertEqual(command_events[0]["attempt"], 1)
        self.assertIn("out1", stdout)
        self.assertIn("out2", stdout)
        self.assertIn("err1", stderr)
        self.assertEqual([line for stream, line in line_events if stream == "stdout"], ["out1", "out2"])
        self.assertEqual([line for stream, line in line_events if stream == "stderr"], ["err1"])

    def test_run_scai_command_retries_on_license_errors(self) -> None:
        command_events: list[dict] = []
        line_events: list[tuple[str, str]] = []

        first = _FakeProcess(
            returncode=1,
            stdout_lines=[b""],
            stderr_lines=[b"License validation failed\n"],
        )
        second = _FakeProcess(
            returncode=0,
            stdout_lines=[b"ok\n"],
            stderr_lines=[],
        )

        with patch("agentic_core.nodes.subprocess.Popen", side_effect=[first, second]) as popen:
            with patch("agentic_core.nodes.time.sleep", return_value=None) as sleep:
                code, stdout, stderr = nodes._run_scai_command(
                    ["scai", "code", "convert"],
                    cwd=".",
                    max_retries=2,
                    on_command=lambda payload: command_events.append(payload),
                    on_line=lambda stream, text: line_events.append((stream, text)),
                )

        self.assertEqual(code, 0)
        self.assertEqual(stdout, "ok")
        self.assertEqual(stderr, "")
        self.assertEqual(popen.call_count, 2)
        sleep.assert_called_once()
        self.assertEqual([payload["attempt"] for payload in command_events], [1, 2])
        self.assertIn(("stderr", "License validation failed"), line_events)
        self.assertIn(("stdout", "ok"), line_events)


if __name__ == "__main__":
    unittest.main()

