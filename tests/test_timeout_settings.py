"""Timeout settings can be saved without touching the real .env file."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import webui


class TimeoutSettingsTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.env_file = Path(self.temp_dir.name) / ".env"
        self.env_file.write_text("DOWNLOAD_CONNECT_TIMEOUT=5\nDOWNLOAD_READ_TIMEOUT=15\n", encoding="utf-8")
        env_patch = patch.dict(os.environ, {
            "DOWNLOAD_CONNECT_TIMEOUT": "5", "DOWNLOAD_READ_TIMEOUT": "15",
            "UNLIM_TIMEOUT_RETRIES": "0",
        })
        file_patch = patch.object(webui, "ENV_FILE", self.env_file)
        env_patch.start()
        file_patch.start()
        self.addCleanup(env_patch.stop)
        self.addCleanup(file_patch.stop)
        self.client = TestClient(webui.app)

    def test_save_roundtrip_and_status(self):
        response = self.client.post("/api/config/download_timeouts", json={
            "connect_timeout": 2.5, "read_timeout": 45,
            "unlim_timeout_retries": 2,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["download_read_timeout"], 45)
        self.assertEqual(response.json()["unlim_timeout_retries"], 2)
        self.assertIn("DOWNLOAD_CONNECT_TIMEOUT=2.5", self.env_file.read_text())
        self.assertIn("DOWNLOAD_READ_TIMEOUT=45.0", self.env_file.read_text())
        self.assertIn("UNLIM_TIMEOUT_RETRIES=2", self.env_file.read_text())
        self.assertEqual(os.environ["DOWNLOAD_CONNECT_TIMEOUT"], "2.5")
        self.assertEqual(os.environ["DOWNLOAD_READ_TIMEOUT"], "45.0")
        self.assertEqual(os.environ["UNLIM_TIMEOUT_RETRIES"], "2")

        with patch.object(webui, "check_token", return_value={"ok": False}), \
             patch.object(webui, "check_cookies", return_value={"ok": False}), \
             patch.object(webui, "db_stats", return_value={}), \
             patch.object(webui, "local_size", return_value=0), \
             patch.object(webui.tm, "status", return_value={}), \
             patch.object(webui.pipeline, "status", return_value={}):
            status = self.client.get("/api/status")
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.json()["config"]["download_connect_timeout"], 2.5)
        self.assertEqual(status.json()["config"]["download_read_timeout"], 45)
        self.assertEqual(status.json()["config"]["unlim_timeout_retries"], 2)

    def test_legacy_timeout_update_preserves_retry_setting(self):
        self.env_file.write_text(
            "DOWNLOAD_CONNECT_TIMEOUT=5\nDOWNLOAD_READ_TIMEOUT=15\nUNLIM_TIMEOUT_RETRIES=4\n",
            encoding="utf-8",
        )
        os.environ["UNLIM_TIMEOUT_RETRIES"] = "4"
        response = self.client.post("/api/config/download_timeouts", json={
            "connect_timeout": 3, "read_timeout": 30,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["unlim_timeout_retries"], 4)
        self.assertIn("UNLIM_TIMEOUT_RETRIES=4", self.env_file.read_text())
        self.assertEqual(os.environ["UNLIM_TIMEOUT_RETRIES"], "4")

    def test_retry_boundary_values_roundtrip(self):
        for retries in (0, 10):
            with self.subTest(retries=retries):
                response = self.client.post("/api/config/download_timeouts", json={
                    "connect_timeout": 5, "read_timeout": 15,
                    "unlim_timeout_retries": retries,
                })
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["unlim_timeout_retries"], retries)
                self.assertEqual(webui.get_config()["unlim_timeout_retries"], retries)

    def test_invalid_retry_does_not_mutate_file_or_environment(self):
        original = self.env_file.read_text()
        for bad in (-1, 11, True, False, "2", 1.5, None):
            with self.subTest(bad=bad):
                response = self.client.post("/api/config/download_timeouts", json={
                    "connect_timeout": 3, "read_timeout": 30,
                    "unlim_timeout_retries": bad,
                })
                self.assertEqual(response.status_code, 422)
                self.assertEqual(self.env_file.read_text(), original)
                self.assertEqual(os.environ["DOWNLOAD_CONNECT_TIMEOUT"], "5")
                self.assertEqual(os.environ["DOWNLOAD_READ_TIMEOUT"], "15")
                self.assertEqual(os.environ["UNLIM_TIMEOUT_RETRIES"], "0")

    def test_invalid_payload_does_not_mutate_file_or_environment(self):
        original = self.env_file.read_text()
        for bad in (0, -1, 3601, "NaN", "Infinity", "oops", None):
            with self.subTest(bad=bad):
                response = self.client.post("/api/config/download_timeouts", json={
                    "connect_timeout": bad, "read_timeout": 20,
                })
                self.assertEqual(response.status_code, 422)
                self.assertEqual(self.env_file.read_text(), original)
                self.assertEqual(os.environ["DOWNLOAD_CONNECT_TIMEOUT"], "5")
                self.assertEqual(os.environ["DOWNLOAD_READ_TIMEOUT"], "15")

    def test_invalid_environment_uses_defaults(self):
        self.env_file.write_text("DOWNLOAD_CONNECT_TIMEOUT=bad\nDOWNLOAD_READ_TIMEOUT=inf\n", encoding="utf-8")
        cfg = webui.get_config()
        self.assertEqual(cfg["download_connect_timeout"], 5)
        self.assertEqual(cfg["download_read_timeout"], 15)
        self.assertEqual(cfg["unlim_timeout_retries"], 0)


if __name__ == "__main__":
    unittest.main()
