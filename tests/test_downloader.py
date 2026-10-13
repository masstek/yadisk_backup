import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yadisk

from downloader import DownloadJob, _download_one, _is_retryable, rest_request_args, scan_tree
from state import State


def md5(data):
    return hashlib.md5(data).hexdigest()


class FakeClient:
    def __init__(self, data, fail=False):
        self.data = data
        self.fail = fail
        self.calls = 0

    def download(self, remote, path):
        self.calls += 1
        with open(path, "wb") as output:
            output.write(self.data)
        if self.fail:
            raise OSError("connection lost")


class DownloaderTests(unittest.TestCase):
    def test_failed_transfer_keeps_old_copy_and_removes_part(self):
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "file.bin")
            Path(target).write_bytes(b"old copy")
            job = DownloadJob("disk:/file.bin", 8, md5(b"new copy"), target)
            client = FakeClient(b"partial", fail=True)
            result = _download_one(job, client, root)
            self.assertEqual(result.status, "failed")
            self.assertEqual(client.calls, 1)
            self.assertEqual(Path(target).read_bytes(), b"old copy")
            self.assertEqual(sorted(os.listdir(root)), ["file.bin"])

    def test_size_and_md5_verified_before_replace(self):
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "file.bin")
            Path(target).write_bytes(b"original")
            job = DownloadJob("disk:/file.bin", 8, md5(b"expected"), target)
            for payload in (b"short", b"badbytes"):
                result = _download_one(job, FakeClient(payload), root)
                self.assertEqual(result.status, "failed")
                self.assertEqual(Path(target).read_bytes(), b"original")
            result = _download_one(job, FakeClient(b"expected"), root)
            self.assertEqual(result.status, "downloaded")
            self.assertEqual(Path(target).read_bytes(), b"expected")

    def test_network_error_does_not_persist_signed_url(self):
        with tempfile.TemporaryDirectory() as root:
            class Client:
                def download(self, remote, path):
                    raise OSError("https://example.test/?token=secret")
            result = _download_one(DownloadJob("disk:/file", 1, None), Client(), root)
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error, "OSError")

    def test_existing_bytes_need_md5_to_skip(self):
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "file.bin")
            Path(target).write_bytes(b"wrong")
            job = DownloadJob("disk:/file.bin", 5, md5(b"right"), target)
            client = FakeClient(b"right")
            self.assertEqual(_download_one(job, client, root).status, "downloaded")
            self.assertEqual(_download_one(job, client, root).status, "skipped")
            self.assertEqual(client.calls, 1)

    def test_scan_failure_is_reported(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            class Client:
                def listdir(self, path, max_items=None):
                    if path == "/":
                        return [SimpleNamespace(path="disk:/child", type="dir")]
                    raise yadisk.exceptions.PathNotFoundError()
            with self.assertRaisesRegex(RuntimeError, "Обход неполон"):
                scan_tree(Client(), state)
            state.close()

    def test_retry_classifier_rejects_permanent_errors(self):
        self.assertFalse(_is_retryable(yadisk.exceptions.UnauthorizedError()))
        self.assertFalse(_is_retryable(yadisk.exceptions.ForbiddenError()))
        self.assertFalse(_is_retryable(yadisk.exceptions.PathNotFoundError()))
        self.assertFalse(_is_retryable(yadisk.exceptions.YaDiskError()))
        self.assertTrue(_is_retryable(yadisk.exceptions.TooManyRequestsError()))
        self.assertTrue(_is_retryable(yadisk.exceptions.RetriableYaDiskError()))
        self.assertFalse(_is_retryable(yadisk.exceptions.RetriableYaDiskError(disable_retry=True)))

    def test_rest_timeouts_are_read_when_client_is_created(self):
        with patch.dict(os.environ, {"DOWNLOAD_CONNECT_TIMEOUT": "3", "DOWNLOAD_READ_TIMEOUT": "9"}):
            self.assertEqual(rest_request_args(), {"timeout": (3.0, 9.0), "n_retries": 0})
        with patch.dict(os.environ, {"DOWNLOAD_CONNECT_TIMEOUT": "0", "DOWNLOAD_READ_TIMEOUT": "bad"}):
            self.assertEqual(rest_request_args(), {"timeout": (5.0, 15.0), "n_retries": 0})


if __name__ == "__main__":
    unittest.main()
