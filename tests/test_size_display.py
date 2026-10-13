"""Size accounting keeps unknown metadata distinct from a real zero-byte file."""

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import webui


class SizeDisplayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_file = Path(self.tmp.name) / "files.db"
        with closing(sqlite3.connect(self.db_file)) as db:
            db.execute(
                "CREATE TABLE files (remote_path TEXT PRIMARY KEY, "
                "size INTEGER, status TEXT, downloaded_at TEXT)"
            )

    def insert(self, path, size, status="pending"):
        with closing(sqlite3.connect(self.db_file)) as db:
            db.execute(
                "INSERT INTO files VALUES (?, ?, ?, NULL)", (path, size, status)
            )
            db.commit()

    def test_unknown_and_zero_are_distinct(self):
        self.insert("/unknown.jpg", None)
        self.insert("/empty.jpg", 0, "downloaded")
        preview = webui.preview_db(self.db_file, "files", "remote_path")
        photos = preview["categories"]["photos"]
        self.assertEqual(
            (photos["count"], photos["bytes"], photos["known_size_count"],
             photos["unknown_size_count"]),
            (2, 0, 1, 1),
        )
        stats = webui.db_stats(self.db_file)
        self.assertEqual((stats["total"], stats["bytes_total"],
                          stats["known_size_count"], stats["unknown_size_count"]),
                         (2, 0, 1, 1))

    def test_mixed_categories_and_downloaded_bytes(self):
        self.insert("/photo.jpg", None)
        self.insert("/film.mp4", 1024, "downloaded")
        self.insert("/song.mp3", 2048, "skipped")
        preview = webui.preview_db(self.db_file, "files", "remote_path")
        self.assertEqual(preview["categories"]["photos"]["unknown_size_count"], 1)
        self.assertEqual(preview["categories"]["videos"]["bytes"], 1024)
        self.assertEqual(preview["categories"]["audio"]["known_size_count"], 1)
        stats = webui.db_stats(self.db_file)
        self.assertEqual((stats["bytes_total"], stats["bytes_done"],
                          stats["known_size_count"], stats["unknown_size_count"]),
                         (3072, 3072, 2, 1))

    def test_all_known_and_empty_defaults(self):
        self.insert("/empty.jpg", 0)
        self.insert("/film.mp4", 512)
        preview = webui.preview_db(self.db_file, "files", "remote_path")
        self.assertEqual(preview["categories"]["photos"]["unknown_size_count"], 0)
        self.assertEqual(preview["categories"]["videos"]["known_size_count"], 1)
        self.assertEqual(webui.db_stats(self.db_file)["unknown_size_count"], 0)

        missing = Path(self.tmp.name) / "missing.db"
        self.assertEqual(webui.db_stats(missing)["known_size_count"], 0)
        self.assertEqual(webui.db_stats(missing)["unknown_size_count"], 0)
        empty_preview = webui.preview_db(missing, "files", "remote_path")
        self.assertTrue(empty_preview["empty"])
        self.assertEqual(empty_preview["total"], 0)
        self.assertEqual(empty_preview["categories"]["photos"]["known_size_count"], 0)
        self.assertEqual(empty_preview["categories"]["photos"]["unknown_size_count"], 0)


if __name__ == "__main__":
    unittest.main()
