"""Tests for safe stdout diagnostics."""

import io
import time
import unittest
from contextlib import redirect_stdout

from server_diagnostics import DiagnosticsMonitor, emit, redact, relay


class RedactionTests(unittest.TestCase):
    def test_urls_and_credentials_are_removed(self):
        original = (
            "fetch https://example.test/a?token=private123 "
            "token=private456 cookie=session789 Authorization: Bearer abc123 "
            'json={"uid":"user42"} y0_AgAAAAsecret AQVNmysecret sk-abcdefghijk'
        )
        safe = redact(original)
        for secret in ("example.test", "private123", "private456", "session789", "abc123",
                       "user42", "y0_AgAAAAsecret", "AQVNmysecret", "sk-abcdefghijk"):
            self.assertNotIn(secret, safe)
        self.assertIn("[URL_REDACTED]", safe)

    def test_cookie_header_hides_all_pairs(self):
        safe = redact("Cookie: Session_id=first-secret; sessionid2=second-secret")
        self.assertNotIn("first-secret", safe)
        self.assertNotIn("second-secret", safe)
        self.assertNotIn("sid", safe.lower())

    def test_authorization_header_and_session_assignments_are_masked(self):
        safe = redact("Authorization: OAuth secret-a, extra=secret-b")
        self.assertNotIn("secret-a", safe)
        self.assertNotIn("secret-b", safe)
        assignments = redact("Session_id=secret1 sessionid2=secret2 yandexuid=secret3")
        for secret in ("secret1", "secret2", "secret3"):
            self.assertNotIn(secret, assignments)

    def test_progress_text_remains_readable(self):
        self.assertEqual(redact("Downloaded 18/27 files (66%)"), "Downloaded 18/27 files (66%)")

    def test_emit_uses_current_stdout_and_flushes(self):
        output = io.StringIO()
        with redirect_stdout(output):
            emit("server_started", port=8765, token="do-not-print")
        line = output.getvalue()
        self.assertRegex(line, r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d[+-]\d\d:\d\d server_started ")
        self.assertIn('port="8765"', line)
        self.assertNotIn("do-not-print", line)
        self.assertTrue(line.endswith("\n"))

    def test_relay_keeps_message_and_masks_secrets(self):
        output = io.StringIO()
        with redirect_stdout(output):
            relay("disk", "Downloaded 3 files; token=secret123\n")
        self.assertIn("Downloaded 3 files", output.getvalue())
        self.assertIn("child_output", output.getvalue())
        self.assertNotIn("secret123", output.getvalue())


class MonitorTests(unittest.TestCase):
    def test_report_emits_heartbeat_when_idle_and_task_status_when_active(self):
        current = []
        monitor = DiagnosticsMonitor(lambda: current)
        output = io.StringIO()
        with redirect_stdout(output):
            monitor.report()
            current.append({"slot": "disk", "pid": 12, "running": True,
                            "elapsed": 61.25, "idle": 7.25})
            monitor.report()
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 3)
        self.assertIn('active="0"', lines[0])
        self.assertIn('active="1"', lines[1])
        self.assertIn('slot="disk"', lines[2])
        self.assertIn('elapsed_s="61.2"', lines[2])
        self.assertIn('idle_s="7.2"', lines[2])

    def test_start_is_idempotent_and_stop_ends_thread(self):
        monitor = DiagnosticsMonitor(lambda: [], interval=0.01)
        output = io.StringIO()
        with redirect_stdout(output):
            monitor.start()
            first = monitor._thread
            monitor.start()
            self.assertIs(monitor._thread, first)
            time.sleep(0.04)
            monitor.stop()
        self.assertFalse(first.is_alive())
        self.assertIn("server_heartbeat", output.getvalue())

    def test_snapshot_failure_does_not_kill_monitor(self):
        def broken():
            raise ValueError("secret reason")

        output = io.StringIO()
        with redirect_stdout(output):
            DiagnosticsMonitor(broken).report()
        self.assertIn('error="ValueError"', output.getvalue())
        self.assertNotIn("secret reason", output.getvalue())


if __name__ == "__main__":
    unittest.main()
