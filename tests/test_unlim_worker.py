"""Upstream worker regressions adapted from ZIP batches to single ORIGINAL files.

Path containment, identity, completeness, exclusive ownership and bounded failure
remain required; matching archive members by filename/size is intentionally gone.
"""

import hashlib
import os
import sqlite3
import threading
from unittest.mock import Mock

import pytest

import unlim_state as queue
import unlim_worker as uw
from unlim_api import DownloadError, Timeouts


@pytest.mark.parametrize(
    "name",
    [
        "2020/photo.jpg",
        "../../evil.txt",
        "C:/evil.txt",
        "/etc/passwd",
        r"..\..\x",
        "//server/share/x",
        "./x",
        "a/../../x",
        "../..",
        "",
    ],
)
def test_original_destination_stays_inside_root(tmp_path, name):
    root = tmp_path / "extract"
    dst = uw.file_destination(root, {"file_id": "photounlim:/one", "name": name})
    assert os.path.commonpath(
        [os.path.realpath(root), os.path.realpath(dst)]
    ) == os.path.realpath(root)


def test_same_name_and_size_have_distinct_destinations(tmp_path):
    jobs = [{"file_id": fid, "name": "a.jpg", "size": 100} for fid in ("one", "two")]
    assert uw.file_destination(tmp_path, jobs[0]) != uw.file_destination(
        tmp_path, jobs[1]
    )


def test_size_alone_cannot_confirm_original(tmp_path):
    target = tmp_path / "a.jpg"
    target.write_bytes(b"data")
    assert uw.verified_existing(target, 4, None) is None
    assert uw.verified_existing(target, 4, hashlib.md5(b"evil").hexdigest()) is None
    assert uw.verified_existing(target, 4, hashlib.md5(b"data").hexdigest()) == (
        4,
        hashlib.sha256(b"data").hexdigest(),
    )


@pytest.fixture()
def db(tmp_path):
    path = str(tmp_path / "unlim_state.db")
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE unlim_files (
            file_id TEXT PRIMARY KEY, cluster_id TEXT, name TEXT, size INTEGER,
            md5 TEXT, mime TEXT, status TEXT DEFAULT 'pending', local_path TEXT,
            error TEXT, worker_id INTEGER, assigned_at TIMESTAMP,
            downloaded_at TIMESTAMP
        );
        INSERT INTO unlim_files(file_id,name,size) VALUES
            ('1','a.jpg',10), ('2','b.jpg',20);
    """)
    con.close()
    queue.ensure_schema(path)
    return path


def test_migration_adds_attempts_and_lease_columns(db):
    with sqlite3.connect(db) as con:
        cols = {r[1] for r in con.execute("PRAGMA table_info(unlim_files)")}
    assert {"attempts", "lease_token", "lease_until", "download_path", "sha256"} <= cols


def test_claim_returns_metadata_and_exclusively_owns_one_file(db):
    first = queue.claim(db, worker_id=1, run_id="run")
    second = queue.claim(db, worker_id=2, run_id="run")
    assert {first["file_id"], second["file_id"]} == {"1", "2"}
    assert all(
        "name" in job and "size" in job and job["lease_token"]
        for job in [first, second]
    )
    assert queue.claim(db, worker_id=3, run_id="run") is None


def test_failure_counts_attempt_and_waits_for_next_run(db):
    first = queue.claim(db, 1, "run1")
    queue.fail(db, first, "boom")
    second = queue.claim(db, 1, "run1")
    assert second["file_id"] != first["file_id"]
    queue.fail(db, second, "boom")
    assert queue.claim(db, 1, "run1") is None
    retried = queue.claim(db, 1, "run2")
    assert retried["attempts"] == 2


def test_complete_records_only_owned_original(db, tmp_path):
    job = queue.claim(db, 1, "run")
    target = tmp_path / "a.jpg"
    target.write_bytes(b"data")
    queue.complete(
        db, job, str(target), hashlib.sha256(b"data").hexdigest(), actual_size=4
    )
    with sqlite3.connect(db) as con:
        rows = dict(con.execute("SELECT file_id,status FROM unlim_files"))
        local = con.execute(
            "SELECT local_path FROM unlim_files WHERE file_id=?", (job["file_id"],)
        ).fetchone()[0]
    assert rows == {"1": "downloaded", "2": "pending"}
    assert local == str(target)


def test_large_files_are_claimed_individually(db):
    with sqlite3.connect(db) as con:
        con.execute("UPDATE unlim_files SET size=500000000 WHERE file_id='1'")
    first = queue.claim(db, 1, "run")
    second = queue.claim(db, 2, "run")
    assert first["file_id"] != second["file_id"]
    assert sum(job["size"] for job in [first, second]) == 500000020


def test_pending_precedes_previously_failed_original(db):
    with sqlite3.connect(db) as con:
        con.execute(
            "UPDATE unlim_files SET status='failed', attempts=1 WHERE file_id='1'"
        )
    assert queue.claim(db, 1, "run")["file_id"] == "2"
    assert queue.claim(db, 1, "run")["file_id"] == "1"


def test_incomplete_original_cannot_be_confirmed(tmp_path):
    response = Mock(status_code=200, headers={})
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.iter_content.return_value = [b"partial"]
    session = Mock()
    session.get.return_value = response
    with pytest.raises(DownloadError, match="не полностью"):
        uw.download_file(
            session,
            "https://storage.invalid/original",
            str(tmp_path / "a.part"),
            100,
            None,
            Timeouts(),
            threading.Event(),
        )


def test_original_transfer_never_extracts_archive_members(tmp_path):
    # A ZIP original is an opaque file; no archive member becomes a local pathname.
    import io
    import zipfile

    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("../../escaped.txt", b"bad")
        archive.writestr("C:/escaped2.txt", b"bad")
    data = payload.getvalue()
    response = Mock(status_code=200, headers={"Content-Length": str(len(data))})
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.iter_content.return_value = [data]
    session = Mock()
    session.get.return_value = response
    target = tmp_path / "original.part"
    size, sha = uw.download_file(
        session,
        "https://storage.invalid/original",
        str(target),
        len(data),
        hashlib.md5(data).hexdigest(),
        Timeouts(),
        threading.Event(),
    )
    assert size == len(data)
    assert sha == hashlib.sha256(data).hexdigest()
    assert target.read_bytes() == data
    assert list(tmp_path.iterdir()) == [target]
