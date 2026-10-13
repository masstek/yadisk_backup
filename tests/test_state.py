import hashlib
import os
import tempfile
import unittest
from pathlib import Path

from state import State
from utils import unique_local_path


class StateTests(unittest.TestCase):
    def test_changed_or_missing_file_reopens_completed_row(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            local = os.path.join(root, "copy")
            Path(local).write_bytes(b"old")
            state.mark_pending("disk:/a", 3, "oldhash")
            state.mark_downloaded("disk:/a", local)
            state.mark_pending("disk:/a", 3, "oldhash")
            self.assertEqual(state.get_stats()["pending"], 0)
            state.mark_pending("disk:/a", 3, "newhash")
            self.assertEqual(state.get_stats()["pending"], 1)
            state.mark_downloaded("disk:/a", local)
            os.remove(local)
            state.mark_pending("disk:/a", 3, "newhash")
            self.assertEqual(state.get_stats()["pending"], 1)
            state.mark_downloaded("disk:/a", local)
            Path(local).write_bytes(b"wrong size")
            state.mark_pending("disk:/a", 3, "newhash")
            self.assertEqual(state.get_stats()["pending"], 1)
            state.close()

    def test_reservations_survive_sanitization_and_case_collision(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            remotes = ["disk:/A?.jpg", "disk:/A*.jpg", "disk:/foo.jpg", "disk:/FOO.jpg"]
            for remote in remotes:
                state.mark_pending(remote, 1, hashlib.md5(remote.encode()).hexdigest())
            paths = state.reserve_local_paths(root, remotes)
            self.assertEqual(len({p.casefold() for p in paths.values()}), len(remotes))
            self.assertEqual(paths, state.reserve_local_paths(root, list(reversed(remotes))))
            state.close()

    def test_legacy_completed_path_is_preserved(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            legacy = os.path.join(root, "old.jpg")
            state.mark_pending("disk:/old.jpg", 3, None)
            state.mark_downloaded("disk:/old.jpg", legacy)
            state.mark_pending("disk:/old.jpg", 4, None)
            self.assertEqual(state.reserve_local_paths(root, ["disk:/old.jpg"])["disk:/old.jpg"], legacy)
            state.close()

    def test_failed_is_retried_on_later_run(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            state.mark_pending("disk:/unavailable", 1, None)
            state.mark_failed("disk:/unavailable", "timeout")
            self.assertEqual(state.get_stats()["pending"], 0)
            self.assertEqual(state.reset_failed_to_pending(), 1)
            self.assertEqual(state.get_stats()["pending"], 1)
            state.close()

    def test_reservation_does_not_claim_untracked_file(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            remote = "disk:/file.txt"
            state.mark_pending(remote, 1, None)
            occupied = unique_local_path(root, remote)
            Path(occupied).write_text("someone else's file")
            reserved = state.reserve_local_paths(root, [remote])[remote]
            self.assertNotEqual(reserved, occupied)
            self.assertEqual(Path(occupied).read_text(), "someone else's file")
            state.close()

    def test_new_remote_md5_rechecks_existing_copy(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            local = os.path.join(root, "copy")
            Path(local).write_bytes(b"data")
            state.mark_pending("disk:/a", 4, None)
            state.mark_downloaded("disk:/a", local)
            state.mark_pending("disk:/a", 4, hashlib.md5(b"data").hexdigest())
            self.assertEqual(state.get_stats()["pending"], 1)
            state.close()

    def test_legacy_colliding_paths_requeue_unverified_row(self):
        with tempfile.TemporaryDirectory() as root:
            state = State(os.path.join(root, "state.db"))
            local = os.path.join(root, "same.jpg")
            Path(local).write_bytes(b"first")
            first, second = "disk:/a?.jpg", "disk:/a*.jpg"
            state.mark_pending(first, 5, hashlib.md5(b"first").hexdigest())
            state.mark_pending(second, 6, hashlib.md5(b"second").hexdigest())
            state.mark_downloaded(first, local)
            state.mark_downloaded(second, local)
            self.assertEqual(state.repair_colliding_local_paths(), 1)
            rows = {r["remote_path"]: r for r in state._conn().execute("SELECT * FROM files")}
            self.assertEqual(rows[first]["local_path"], local)
            self.assertEqual(rows[second]["status"], "pending")
            self.assertIsNone(rows[second]["local_path"])
            state.close()


if __name__ == "__main__":
    unittest.main()
