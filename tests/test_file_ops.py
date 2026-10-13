import os
from contextlib import closing
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import organize
from state import State


class FileOpsTests(unittest.TestCase):
    def test_organize_move_updates_manifest(self):
        with tempfile.TemporaryDirectory() as workspace:
            root = os.path.join(workspace, "downloads")
            os.mkdir(root)
            db = os.path.join(workspace, "state.db")
            src = os.path.join(root, "document.txt")
            Path(src).write_text("content")
            state = State(db)
            state.mark_pending("disk:/document.txt", 7, None)
            state.mark_downloaded("disk:/document.txt", src)
            state.close()
            unlim_db = os.path.join(workspace, "unlim_state.db")
            with closing(sqlite3.connect(unlim_db)) as conn:
                conn.execute("CREATE TABLE unlim_files (file_id TEXT PRIMARY KEY, local_path TEXT)")
                conn.execute("INSERT INTO unlim_files VALUES (?, ?)", ("photo-1", src))
                conn.commit()
            with patch("sys.argv", ["organize.py", "--input", root, "--state-db", db,
                                    "--unlim-state-db", unlim_db, "--action", "move"]):
                self.assertEqual(organize.main(), 0)
            with closing(sqlite3.connect(db)) as conn:
                local = conn.execute("SELECT local_path FROM files").fetchone()[0]
            with closing(sqlite3.connect(unlim_db)) as conn:
                unlim_local = conn.execute("SELECT local_path FROM unlim_files").fetchone()[0]
            self.assertEqual(local, os.path.join(root, "Документы", "document.txt"))
            self.assertEqual(unlim_local, local)
            self.assertTrue(os.path.isfile(local))
            self.assertFalse(os.path.exists(src))


if __name__ == "__main__":
    unittest.main()
