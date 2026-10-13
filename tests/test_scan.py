import os
import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import main
from downloader import scan_files, scan_media
from state import State


def entry(path, size=1, md5="same"):
    return SimpleNamespace(path=path, type="file", size=size, md5=md5)


class FlatClient:
    def __init__(self, items, fail_offset=None, repeat=False):
        self.items = items
        self.fail_offset = fail_offset
        self.repeat = repeat
        self.calls = []

    def get_files(self, **kwargs):
        self.calls.append(kwargs)
        offset = kwargs["offset"]
        if offset == self.fail_offset:
            raise ValueError("page failed")
        if self.repeat and offset:
            offset = 0
        return iter(self.items[offset:offset + kwargs["max_items"]])

    def close(self):
        pass


class FlatScanTests(unittest.TestCase):
    def test_all_file_types_across_bounded_pages(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            items = [entry(f"disk:/file-{i}.bin") for i in range(2001)]
            items[0] = entry("disk:/audio.mp3", 3)
            items[1] = entry("disk:/photo.jpg", 5)
            items[2] = entry("disk:/text.txt", 7)
            client = FlatClient(items)
            self.assertEqual(scan_files(client, state), (2001, 2013))
            self.assertEqual([c["offset"] for c in client.calls], [0, 1000, 2000])
            for call in client.calls:
                self.assertEqual(call["limit"], 1000)
                self.assertEqual(call["max_items"], 1000)
                self.assertEqual(call["sort"], "path")
                self.assertNotIn("media_type", call)
            self.assertEqual(state.get_stats()["pending"], 2001)
            state.close()

    def test_partial_page_failure_preserves_queued_files(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            client = FlatClient([entry(f"disk:/f-{i}") for i in range(1500)], fail_offset=1000)
            with self.assertRaisesRegex(ValueError, "page failed"):
                scan_files(client, state)
            self.assertEqual(state.get_stats()["pending"], 1000)
            state.close()

    def test_repeated_page_reports_incomplete_scan(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            client = FlatClient([entry(f"disk:/f-{i}") for i in range(1000)], repeat=True)
            with self.assertRaisesRegex(RuntimeError, "повторил страницу"):
                scan_files(client, state)
            self.assertEqual(len(client.calls), 2)
            state.close()

    def test_cyclic_pages_report_incomplete_scan(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            pages = [
                [entry(f"disk:/a-{i}") for i in range(1000)],
                [entry(f"disk:/b-{i}") for i in range(1000)],
            ]

            class CyclicClient:
                calls = 0

                def get_files(self, **kwargs):
                    page = pages[self.calls % 2]
                    self.calls += 1
                    return iter(page)

            client = CyclicClient()
            with self.assertRaisesRegex(RuntimeError, "повторил страницу"):
                scan_files(client, state)
            self.assertEqual(client.calls, 3)
            state.close()

    def test_media_scan_bounds_each_page_and_filters(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            client = FlatClient([entry("disk:/photo.jpg")])
            self.assertEqual(scan_media(client, state, media_types=("image",)), (1, 1))
            self.assertEqual(client.calls[0]["media_type"], "image")
            self.assertEqual(client.calls[0]["max_items"], 1000)
            state.close()

    def test_scan_keeps_existing_downloaded_status(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            local_path = Path(root, "saved.bin")
            local_path.write_bytes(b"hello")
            state.mark_pending("disk:/saved.bin", 5, "same")
            state.mark_downloaded("disk:/saved.bin", str(local_path))
            self.assertEqual(scan_files(FlatClient([entry("disk:/saved.bin", 5)]), state), (1, 5))
            self.assertEqual(state.get_stats()["downloaded"], 1)
            self.assertEqual(state.get_stats()["pending"], 0)
            state.close()

    def test_cli_routes_whole_disk_to_flat_scan_and_folder_to_tree(self):
        with tempfile.TemporaryDirectory() as root:
            db_path = Path(root, "state.db")
            client = FlatClient([entry("disk:/file.txt")])
            with patch.object(main, "DB_FILE", db_path), \
                 patch.object(main, "check_token", return_value=(True, "ok")), \
                 patch.object(main.yadisk, "Client", return_value=client), \
                 patch.object(main, "scan_tree", return_value=(2, 3)) as tree:
                for disk_root in ("/", "disk:/"):
                    self.assertEqual(main.cmd_scan({"token": "fake"}, SimpleNamespace(root=disk_root, roots=None)), 0)
                tree.assert_not_called()
                self.assertEqual(main.cmd_scan({"token": "fake"}, SimpleNamespace(root="/Folder", roots=None)), 0)
                tree.assert_called_once()

    def test_cli_returns_failure_after_partial_scan_and_keeps_state(self):
        with tempfile.TemporaryDirectory() as root:
            db_path = Path(root, "state.db")
            client = FlatClient([entry(f"disk:/f-{i}") for i in range(1500)], fail_offset=1000)
            with patch.object(main, "DB_FILE", db_path), \
                 patch.object(main, "check_token", return_value=(True, "ok")), \
                 patch.object(main.yadisk, "Client", return_value=client), \
                 contextlib.redirect_stdout(io.StringIO()), \
                 contextlib.redirect_stderr(io.StringIO()):
                code = main.cmd_scan({"token": "fake"}, SimpleNamespace(root="/", roots=None))
            self.assertEqual(code, 1)
            state = State(str(db_path))
            self.assertEqual(state.get_stats()["pending"], 1000)
            state.close()

    def test_download_auto_scan_uses_flat_list_for_whole_disk(self):
        with tempfile.TemporaryDirectory() as root:
            db_path = Path(root, "state.db")
            client = FlatClient([entry("disk:/file.txt")])
            cfg = {"token": "fake", "max_workers": 1, "download_dir": root}
            args = SimpleNamespace(root="disk:/", no_scan=False, workers=1)
            counters = {"downloaded": 0, "skipped": 0, "failed": 0, "bytes": 0}
            with patch.object(main, "DB_FILE", db_path), \
                 patch.object(main, "check_token", return_value=(True, "ok")), \
                 patch.object(main.yadisk, "Client", return_value=client), \
                 patch.object(main, "scan_tree") as tree, \
                 patch.object(main, "download_all", return_value=counters), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main.cmd_download(cfg, args), 1)
            self.assertEqual(len(client.calls), 1)
            tree.assert_not_called()


if __name__ == "__main__":
    unittest.main()
