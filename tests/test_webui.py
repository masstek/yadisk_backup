"""Offline regressions for the UI operation dispatcher."""

import tempfile
import threading
import unittest
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import webui


class FakeTaskManager:
    def __init__(self, exit_code=0):
        self._lock = threading.Lock()
        self._tasks = {}
        self.starts = []
        self.exit_code = exit_code

    def start(self, slot, script, args):
        self.starts.append((slot, script, args))
        self._tasks[slot] = SimpleNamespace(
            name=slot,
            alive=lambda: False,
            proc=SimpleNamespace(poll=lambda: self.exit_code),
        )
        return True, "started"

    def stop(self, slot):
        return True, "stopped"

    def active(self, prefixes=()):
        return False


class PipelineTests(unittest.TestCase):
    def test_cancelled_pipeline_cannot_start_new_process(self):
        fake = FakeTaskManager()
        p = webui.Pipeline()
        p._stop.set()
        with patch.object(webui, "tm", fake):
            ok, _ = p._own_start("disk", "main.py", ["scan"])
        self.assertFalse(ok)
        self.assertEqual(fake.starts, [])

    def test_scan_failure_stops_before_download(self):
        fake = FakeTaskManager()
        p = webui.Pipeline()
        with patch.object(webui, "tm", fake), \
             patch.object(webui, "get_config", return_value={"token": "example"}), \
             patch.object(webui, "check_token", return_value={"ok": True}), \
             patch.object(p, "_wait_task", return_value=7):
            p._run()
        self.assertEqual(p.state, "failed")
        self.assertEqual([script for _, script, _ in fake.starts], ["main.py"])

    def test_cancelled_scan_never_launches_next_stage(self):
        fake = FakeTaskManager()
        p = webui.Pipeline()

        def cancel_on_wait(*_):
            p._stop.set()
            return 1

        with patch.object(webui, "tm", fake), \
             patch.object(webui, "get_config", return_value={"token": "example"}), \
             patch.object(webui, "check_token", return_value={"ok": True}), \
             patch.object(p, "_wait_task", side_effect=cancel_on_wait):
            p._run()
        self.assertEqual(p.state, "stopped")
        self.assertEqual(len(fake.starts), 1)

    def test_failed_worker_prevents_done(self):
        fake = FakeTaskManager(exit_code=3)
        p = webui.Pipeline()
        p.skip_disk = True
        p.workers = 1
        with patch.object(webui, "tm", fake), \
             patch.object(webui, "get_config", return_value={"token": "example"}), \
             patch.object(webui, "check_token", return_value={"ok": True}), \
             patch.object(webui, "check_cookies", return_value={"ok": True}), \
             patch.object(p, "_wait_task", return_value=0):
            p._run()
        self.assertEqual(p.state, "failed")
        self.assertEqual(len(fake.starts), 2)
        worker_args = fake.starts[1][2]
        self.assertIn("--run-id", worker_args)

    def test_partial_disk_and_collect_still_process_unlim_and_never_done(self):
        fake = FakeTaskManager(exit_code=1)
        p = webui.Pipeline()
        p.workers = 1
        with tempfile.TemporaryDirectory() as directory:
            disk_db = Path(directory) / "disk.db"
            unlim_db = Path(directory) / "unlim.db"
            disk_db.touch()
            unlim_db.touch()
            with patch.object(webui, "tm", fake), \
                 patch.object(webui, "DB_FILE", disk_db), \
                 patch.object(webui, "UNLIM_DB_FILE", unlim_db), \
                 patch.object(webui, "get_config", return_value={"token": "example"}), \
                 patch.object(webui, "check_token", return_value={"ok": True}), \
                 patch.object(webui, "check_cookies", return_value={"ok": True}), \
                 patch.object(webui, "db_stats", return_value={"pending": 1, "failed": 1}), \
                 patch.object(p, "_wait_task", side_effect=[1, 1, 1]):
                p._run()
        self.assertEqual(p.state, "failed")
        self.assertEqual(p.step, "завершено не полностью")
        self.assertTrue(p.had_errors)
        self.assertEqual([script for _, script, _ in fake.starts],
                         ["main.py", "main.py", "unlim_download.py", "unlim_worker.py"])

    def test_unlim_only_does_not_require_disk_token(self):
        fake = FakeTaskManager()
        p = webui.Pipeline()
        p.skip_disk = True
        p.workers = 1
        with tempfile.TemporaryDirectory() as directory:
            unlim_db = Path(directory) / "unlim.db"
            unlim_db.touch()
            with patch.object(webui, "tm", fake), \
                 patch.object(webui, "UNLIM_DB_FILE", unlim_db), \
                 patch.object(webui, "get_config", return_value={"token": ""}), \
                 patch.object(webui, "check_token") as token_check, \
                 patch.object(webui, "check_cookies", return_value={"ok": True}), \
                 patch.object(webui, "db_stats", return_value={}), \
                 patch.object(p, "_wait_task", return_value=0):
                p._run()
        token_check.assert_not_called()
        self.assertEqual(p.state, "done")


class LifespanTests(unittest.IsolatedAsyncioTestCase):
    async def test_shutdown_tasks_runs_on_normal_exit(self):
        with patch.object(webui, "shutdown_tasks") as shutdown:
            async with webui.app.router.lifespan_context(webui.app):
                shutdown.assert_not_called()
        shutdown.assert_called_once_with()

    async def test_shutdown_tasks_runs_when_lifespan_body_raises(self):
        with patch.object(webui, "shutdown_tasks") as shutdown:
            with self.assertRaisesRegex(RuntimeError, "startup failure"):
                async with webui.app.router.lifespan_context(webui.app):
                    raise RuntimeError("startup failure")
        shutdown.assert_called_once_with()


class EndpointTests(unittest.TestCase):
    def test_status_never_returns_raw_token(self):
        with patch.object(webui, "get_config", return_value={
            "token": "secret-token-value", "download_dir": "/tmp/downloads",
            "max_workers": 4,
        }), patch.object(webui, "check_token", return_value={"ok": True}), \
             patch.object(webui, "check_cookies", return_value={"ok": False}), \
             patch.object(webui, "db_stats", return_value={}), \
             patch.object(webui, "local_size", return_value={}), \
             patch.object(webui.tm, "status", return_value={}), \
             patch.object(webui.pipeline, "status", return_value={}):
            result = webui.api_status()
        self.assertNotIn("token", result["config"])
        self.assertNotIn("secret-token-value", str(result))

    def test_selective_busy_does_not_change_database(self):
        with patch.object(webui.pipeline, "is_running", return_value=True), \
             patch.object(webui, "unskip_category_marker") as unskip, \
             patch.object(webui, "filter_to_categories") as filter_rows:
            result = webui.disk_download_selected(
                webui.SelectivePayload(categories=["photos"]))
        self.assertFalse(result["ok"])
        unskip.assert_not_called()
        filter_rows.assert_not_called()

    def test_clear_busy_keeps_database(self):
        with tempfile.TemporaryDirectory() as directory:
            db_file = Path(directory) / "state.db"
            db_file.write_bytes(b"untouched")
            with patch.object(webui, "DB_FILE", db_file), \
                 patch.object(webui.pipeline, "is_running", return_value=True):
                result = webui.disk_clear()
            self.assertFalse(result["ok"])
            self.assertEqual(db_file.read_bytes(), b"untouched")

    def test_zero_started_workers_is_failure(self):
        with patch.object(webui.tm, "start", return_value=(False, "busy")):
            result = webui._start_workers(2)
        self.assertFalse(result["ok"])
        self.assertEqual(result["started"], [])
        self.assertEqual(len(result["failed"]), 1)

    def test_partial_worker_start_stops_started_workers(self):
        with patch.object(webui.tm, "start", side_effect=[
            (True, "started"), (False, "spawn error")
        ]), patch.object(webui.tm, "stop", return_value=(True, "stopped")) as stop:
            result = webui._start_workers(3)
        self.assertFalse(result["ok"])
        self.assertEqual(result["started"], [])
        stop.assert_called_once_with("unlim_w1")

    def test_stop_all_cancels_pipeline_before_stopping_tasks(self):
        events = []
        with patch.object(webui.pipeline, "stop", side_effect=lambda: events.append("cancel")), \
             patch.object(webui.tm, "stop_all", side_effect=lambda: events.append("stop") or 2):
            result = webui.stop_all()
        self.assertEqual(events, ["cancel", "stop"])
        self.assertEqual(result["stopped"], 2)

    def test_workers_share_one_run_id(self):
        calls = []

        def start(slot, script, args):
            calls.append(args)
            return True, "started"

        with patch.object(webui.tm, "start", side_effect=start):
            result = webui._start_workers(3)
        self.assertTrue(result["ok"])
        self.assertEqual(len({a[a.index("--run-id") + 1] for a in calls}), 1)

    def test_unlim_categories_use_name_instead_of_file_id(self):
        with tempfile.TemporaryDirectory() as directory:
            db_file = Path(directory) / "unlim.db"
            with closing(sqlite3.connect(db_file)) as conn:
                conn.execute("CREATE TABLE unlim_files (file_id TEXT PRIMARY KEY, name TEXT, "
                             "size INTEGER, status TEXT, error TEXT)")
                conn.execute("INSERT INTO unlim_files VALUES "
                             "('opaque-1', 'photo.jpg', 10, 'pending', NULL)")
                conn.execute("INSERT INTO unlim_files VALUES "
                             "('opaque-2', 'clip.mp4', 10, 'failed', 'old error')")
                conn.commit()
            preview = webui.preview_db(db_file, "unlim_files", "file_id")
            self.assertEqual(preview["categories"]["photos"]["count"], 1)
            self.assertEqual(preview["categories"]["videos"]["count"], 1)
            skipped, kept = webui.filter_to_categories(
                db_file, "unlim_files", "file_id", ["photos"])
            self.assertEqual((skipped, kept), (1, 1))
            with closing(sqlite3.connect(db_file)) as conn:
                status, error = conn.execute(
                    "SELECT status, error FROM unlim_files WHERE file_id='opaque-2'"
                ).fetchone()
            self.assertEqual((status, error), ("skipped", "not in selected categories"))


if __name__ == "__main__":
    unittest.main()
