"""SQLite queue for single-file Photoslice downloads; fenced renewable leases."""
from __future__ import annotations

from contextlib import contextmanager
import os
import sqlite3
import time
import uuid

SCHEMA = """
CREATE TABLE IF NOT EXISTS unlim_files (
    file_id TEXT PRIMARY KEY, cluster_id TEXT, name TEXT, size INTEGER,
    md5 TEXT, mime TEXT, status TEXT DEFAULT 'pending', local_path TEXT,
    error TEXT, worker_id INTEGER, assigned_at TIMESTAMP, downloaded_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_unlim_status ON unlim_files(status);
CREATE TABLE IF NOT EXISTS unlim_clusters (
    cluster_id TEXT PRIMARY KEY, size INTEGER, fetched INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS unlim_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


@contextmanager
def connection(path):
    c = sqlite3.connect(path, timeout=60, isolation_level=None)
    c.row_factory = sqlite3.Row
    try:
        c.execute('PRAGMA journal_mode=WAL')
        yield c
    finally:
        c.close()


@contextmanager
def transaction(path):
    with connection(path) as c:
        c.execute('BEGIN IMMEDIATE')
        try:
            yield c
            c.execute('COMMIT')
        except BaseException:
            c.execute('ROLLBACK')
            raise


def ensure_schema(path):
    with connection(path) as c:
        c.executescript(SCHEMA)
    with transaction(path) as c:
        cols = {r['name'] for r in c.execute('PRAGMA table_info(unlim_files)')}
        for name, spec in {
            'attempts': 'INTEGER NOT NULL DEFAULT 0',
            'next_retry_at': 'REAL NOT NULL DEFAULT 0',
            'lease_token': 'TEXT', 'lease_until': 'REAL',
            'sha256': 'TEXT', 'last_run_id': 'TEXT', 'download_path': 'TEXT',
        }.items():
            if name not in cols:
                c.execute(f'ALTER TABLE unlim_files ADD COLUMN {name} {spec}')
        if not c.execute("SELECT 1 FROM unlim_meta WHERE key='single_file_v1'").fetchone():
            # Old ZIP workers marked whole batches complete, with no file mapping.
            # Preserve the old files; download/verify each ID into its own new path.
            c.execute("UPDATE unlim_files SET status='pending', worker_id=NULL, "
                      "error='legacy ZIP result requires individual verification' "
                      "WHERE status='downloaded' AND local_path IS NULL")
            c.execute("UPDATE unlim_files SET status='pending', worker_id=NULL, "
                      "assigned_at=NULL WHERE status='in_progress' AND lease_token IS NULL")
            c.execute("INSERT INTO unlim_meta VALUES ('single_file_v1', '1')")


def reconcile_missing(path):
    """Reopen lost/truncated completed copies, preserving existing filenames."""
    with connection(path) as c:
        rows = list(c.execute("SELECT file_id,local_path,size FROM unlim_files WHERE status='downloaded'"))
    missing = []
    for row in rows:
        try:
            if not row['local_path'] or not os.path.isfile(row['local_path']):
                missing.append(row['file_id'])
            elif row['size'] is not None and os.path.getsize(row['local_path']) != row['size']:
                missing.append(row['file_id'])
        except OSError:
            missing.append(row['file_id'])
    if missing:
        with transaction(path) as c:
            c.executemany("UPDATE unlim_files SET status='pending', error='local copy missing or truncated' "
                          "WHERE file_id=? AND status='downloaded'", [(fid,) for fid in missing])
    return len(missing)


def claim(path, worker_id, run_id, lease_seconds=180):
    now = time.time()
    with transaction(path) as c:
        c.execute("UPDATE unlim_files SET status='failed', lease_token=NULL, lease_until=NULL, "
                  "worker_id=NULL, error='worker lease expired' "
                  "WHERE status='in_progress' AND COALESCE(lease_until,0) < ?", (now,))
        cooldown = c.execute("SELECT value FROM unlim_meta WHERE key='cooldown_until'").fetchone()
        if cooldown and float(cooldown[0]) > now:
            return None
        row = c.execute("SELECT * FROM unlim_files WHERE status IN ('pending','failed') "
                        "AND COALESCE(last_run_id,'') != ? AND next_retry_at<=? "
                        "ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, "
                        "CASE WHEN status='pending' AND (download_path LIKE '/photounlim/%' "
                        "OR (NULLIF(download_path,'') IS NULL AND "
                        "(file_id LIKE 'photounlim:/%' OR file_id LIKE '/photounlim/%'))) "
                        "THEN 0 ELSE 1 END, attempts, file_id LIMIT 1",
                        (run_id, now)).fetchone()
        if row is None:
            return None
        token = uuid.uuid4().hex
        c.execute("UPDATE unlim_files SET status='in_progress', worker_id=?, assigned_at=CURRENT_TIMESTAMP, "
                  "attempts=attempts+1, lease_token=?, lease_until=?, last_run_id=? WHERE file_id=?",
                  (worker_id, token, now + lease_seconds, run_id, row['file_id']))
        job = dict(row)
        job.update(lease_token=token, attempts=row['attempts'] + 1, last_run_id=run_id)
        return job


def renew(path, job, lease_seconds=180):
    with transaction(path) as c:
        return c.execute("UPDATE unlim_files SET lease_until=? WHERE file_id=? AND lease_token=? "
                         "AND status='in_progress' AND lease_until>=?",
                         (time.time() + lease_seconds, job['file_id'], job['lease_token'], time.time())).rowcount == 1


def update_original_metadata(path, job, size, md5):
    """Store fresh original metadata while this worker still owns the job."""
    with transaction(path) as c:
        updated = c.execute(
            "UPDATE unlim_files SET size=COALESCE(?,size), md5=? "
            "WHERE file_id=? AND lease_token=? AND status='in_progress' AND lease_until>=?",
            (size, md5, job['file_id'], job['lease_token'], time.time()),
        ).rowcount
        if updated != 1:
            raise RuntimeError('worker lease lost')


def complete(path, job, local_path, sha256, temporary_path=None, actual_size=None, md5=None):
    """Fence publication as well as DB status so an expired worker cannot overwrite."""
    with transaction(path) as c:
        owned = c.execute("SELECT 1 FROM unlim_files WHERE file_id=? AND lease_token=? "
                          "AND status='in_progress' AND lease_until>=?",
                          (job['file_id'], job['lease_token'], time.time())).fetchone()
        if not owned:
            raise RuntimeError('worker lease lost')
        if temporary_path is not None:
            os.replace(temporary_path, local_path)
        c.execute("UPDATE unlim_files SET status='downloaded', local_path=?, sha256=?, error=NULL, "
                  "downloaded_at=CURRENT_TIMESTAMP, worker_id=NULL, lease_token=NULL, lease_until=NULL, "
                  "next_retry_at=0, size=COALESCE(?,size), md5=? WHERE file_id=? AND lease_token=?",
                  (local_path, sha256, actual_size, md5, job['file_id'], job['lease_token']))


def fail(path, job, error, delay=0, rate_limited=False):
    # Exactly one attempt per file per run, including unavailable server errors.
    with transaction(path) as c:
        c.execute("UPDATE unlim_files SET status='failed', error=?, next_retry_at=?, worker_id=NULL, "
                  "lease_token=NULL, lease_until=NULL WHERE file_id=? AND lease_token=? AND status='in_progress'",
                  (error[:500], time.time() + delay, job['file_id'], job['lease_token']))
        if rate_limited:
            c.execute("INSERT INTO unlim_meta(key,value) VALUES ('cooldown_until',?) "
                      "ON CONFLICT(key) DO UPDATE SET value=CAST(MAX(CAST(value AS REAL), CAST(excluded.value AS REAL)) AS TEXT)",
                      (str(time.time() + delay),))
    return 'failed'


def cooldown_remaining(path):
    with connection(path) as c:
        row = c.execute("SELECT value FROM unlim_meta WHERE key='cooldown_until'").fetchone()
        return max(0, float(row[0]) - time.time()) if row else 0


def release(path, job):
    with transaction(path) as c:
        c.execute("UPDATE unlim_files SET status='pending', worker_id=NULL, lease_token=NULL, lease_until=NULL, "
                  "attempts=MAX(attempts-1,0), last_run_id=NULL WHERE file_id=? AND lease_token=? AND status='in_progress'",
                  (job['file_id'], job['lease_token']))


def retry_failed(path):
    with transaction(path) as c:
        if c.execute("SELECT 1 FROM unlim_files WHERE status='in_progress' AND lease_until>=? LIMIT 1", (time.time(),)).fetchone():
            raise RuntimeError('Сначала остановите все воркеры безлимита')
        return c.execute("UPDATE unlim_files SET status='pending', attempts=0, next_retry_at=0, error=NULL, last_run_id=NULL "
                         "WHERE status='failed'").rowcount


def counts(path):
    with connection(path) as c:
        return {r['status']: r['n'] for r in c.execute('SELECT status, COUNT(*) n FROM unlim_files GROUP BY status')}
