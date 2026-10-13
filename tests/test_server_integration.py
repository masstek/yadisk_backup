import io
import os
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import webui
from downloader import rest_request_args
from runtime_settings import read_timeouts
from unlim_api import Timeouts


class ServerIntegrationTests(unittest.TestCase):
    def test_real_child_inherits_timeouts_and_emits_safe_lifecycle(self):
        manager = webui.TaskManager()
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "child.py"
            script.write_text(
                "import os\n"
                "print('timeouts', os.environ['DOWNLOAD_CONNECT_TIMEOUT'], "
                "os.environ['DOWNLOAD_READ_TIMEOUT'], flush=True)\n"
                "print('retries', os.environ['UNLIM_TIMEOUT_RETRIES'], flush=True)\n"
                "print('URL https://example.invalid/?token=private-secret', flush=True)\n",
                encoding="utf-8",
            )
            cfg = {"download_connect_timeout": 2.5, "download_read_timeout": 600,
                   "unlim_timeout_retries": 2}
            with redirect_stdout(output), patch.object(webui, "get_config", return_value=cfg):
                try:
                    ok, _ = manager.start("smoke", str(script), [])
                    self.assertTrue(ok)
                    task = manager._tasks["smoke"]
                    task._t.join(timeout=5)
                    self.assertFalse(task._t.is_alive())
                    state = manager.status()["smoke"]
                    self.assertFalse(state["running"])
                    self.assertEqual(state["exit_code"], 0)
                    self.assertGreater(state["pid"], 0)
                    self.assertGreaterEqual(state["output_idle"], 0)
                    self.assertEqual(state["elapsed"], manager.status()["smoke"]["elapsed"])
                    events = state["log_events"]
                    self.assertEqual([e["text"] for e in events], state["log_tail"])
                    self.assertEqual([e["seq"] for e in events], sorted(e["seq"] for e in events))
                    self.assertTrue(all(e["at"] > 0 for e in events))
                    with patch.object(webui, "tm", manager):
                        webui.clear_task_log("smoke")
                    self.assertEqual(manager.status()["smoke"]["log_events"], [])
                    self.assertEqual(manager.status()["smoke"]["log_tail"], [])
                finally:
                    manager.stop_all()
                    for task in manager._tasks.values():
                        task.proc.stdout.close()
        log = output.getvalue()
        self.assertIn("task_start", log)
        self.assertIn("task_exit", log)
        self.assertIn("timeouts 2.5 600", log)
        self.assertIn("retries 2", log)
        self.assertNotIn("private-secret", log)
        self.assertNotIn("example.invalid", log)

    def test_snapshot_includes_slow_inflight_http_without_query(self):
        now = time.monotonic()
        requests = {"abc": {"started": now - 10, "path": "/api/status", "method": "GET"}}
        with patch.object(webui, "tm", webui.TaskManager()), \
             patch.object(webui, "_requests_inflight", requests):
            rows = webui.diagnostic_snapshot()
        self.assertEqual(len(rows), 1)
        self.assertIn("http:GET /api/status", rows[0]["slot"])
        self.assertGreaterEqual(rows[0]["elapsed"], 10)

    def test_http_logs_exclude_body_and_query_and_cleanup_requests(self):
        output = io.StringIO()
        with redirect_stdout(output):
            response = TestClient(webui.app).post(
                "/api/unknown?token=query-private", json={"token": "body-private"})
        self.assertEqual(response.status_code, 404)
        log = output.getvalue()
        self.assertIn("http_start", log)
        self.assertIn("http_done", log)
        self.assertNotIn("query-private", log)
        self.assertNotIn("body-private", log)
        self.assertFalse(webui._requests_inflight)

    def test_timeout_readers_agree_for_valid_and_invalid_values(self):
        for raw, expected in (("600", 600), ("0.1", 0.1), ("nan", 5),
                              ("inf", 5), ("bad", 5), ("0", 5), ("3601", 5)):
            with self.subTest(raw=raw), patch.dict(os.environ, {
                "DOWNLOAD_CONNECT_TIMEOUT": raw, "DOWNLOAD_READ_TIMEOUT": "15",
            }):
                self.assertEqual(read_timeouts(), (expected, 15))
                self.assertEqual(Timeouts.from_env().pair, (expected, 15))
                self.assertEqual(rest_request_args()["timeout"], (expected, 15))


if __name__ == "__main__":
    unittest.main()
