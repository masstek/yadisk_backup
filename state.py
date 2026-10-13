"""SQLite-трекер состояния скачивания.

Каждый файл с Яндекс.Диска регистрируется в таблице `files` с одним из статусов:
pending / downloaded / failed / skipped. WAL-режим позволяет писать из нескольких
потоков без блокировок.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import unicodedata
from contextlib import contextmanager
from datetime import datetime
from typing import Iterable, Iterator, Optional

from utils import compute_md5, file_size_safe, unique_local_path

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    remote_path TEXT PRIMARY KEY,
    size INTEGER,
    md5 TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    local_path TEXT,
    error TEXT,
    downloaded_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_status ON files(status);
"""


def _path_key(path: str) -> str:
    return unicodedata.normalize("NFC", os.path.normpath(path)).casefold()


class State:
    """Тонкая обёртка над SQLite. Создаёт по соединению на поток (через threading.local).

    SQLite в WAL умеет конкурентные читатели + один писатель. Чтобы не ловить
    `database is locked`, у каждого потока своё соединение.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._tls = threading.local()
        self._lock = threading.Lock()  # для редких операций над schema
        self._all_conns: list[sqlite3.Connection] = []
        self._all_conns_lock = threading.Lock()
        self._init_schema()

    # ── низкоуровневое ────────────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._tls, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self.db_path,
                timeout=30,
                isolation_level=None,  # autocommit; явные транзакции через BEGIN
                check_same_thread=False,
            )
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.row_factory = sqlite3.Row
            self._tls.conn = conn
            with self._all_conns_lock:
                self._all_conns.append(conn)
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            self._conn().executescript(SCHEMA)

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ── публичное API ─────────────────────────────────────────────────

    def mark_pending(
        self,
        remote_path: str,
        size: Optional[int],
        md5: Optional[str],
    ) -> None:
        """Обновляет метаданные и переоткрывает изменённые/утраченные копии."""
        with self._tx() as conn:
            old = conn.execute(
                "SELECT size, md5, status, local_path FROM files WHERE remote_path=?",
                (remote_path,),
            ).fetchone()
            if old is None:
                conn.execute(
                    "INSERT INTO files (remote_path, size, md5, status) VALUES (?, ?, ?, 'pending')",
                    (remote_path, size, md5),
                )
                return
            effective_md5 = md5 if md5 is not None else (
                old["md5"] if old["size"] == size else None
            )
            changed = (old["size"] != size or
                       (effective_md5 is not None and
                        (old["md5"] is None or old["md5"].lower() != effective_md5.lower())))
            missing = old["status"] in ("downloaded", "skipped") and (
                not old["local_path"] or
                not os.path.isfile(old["local_path"]) or
                (size is not None and file_size_safe(old["local_path"]) != size)
            )
            status = "pending" if changed or missing else old["status"]
            conn.execute(
                "UPDATE files SET size=?, md5=?, status=?, error=CASE WHEN ? THEN NULL ELSE error END, "
                "downloaded_at=CASE WHEN ? THEN NULL ELSE downloaded_at END WHERE remote_path=?",
                (size, effective_md5, status, changed or missing, changed or missing, remote_path),
            )

    def reserve_local_paths(self, download_dir: str, remote_paths: list[str]) -> dict[str, str]:
        """Assign paths before concurrent work; preserve existing manifests when possible."""
        assigned: dict[str, str] = {}
        with self._tx() as conn:
            occupied = {
                _path_key(r["local_path"]): r["remote_path"]
                for r in conn.execute(
                    "SELECT remote_path, local_path FROM files WHERE local_path IS NOT NULL"
                )
            }
            for remote in sorted(set(remote_paths)):
                row = conn.execute(
                    "SELECT local_path FROM files WHERE remote_path=?", (remote,)
                ).fetchone()
                if row is None:
                    raise ValueError(f"Unknown remote path: {remote}")
                previous = row["local_path"]
                if previous and occupied.get(_path_key(previous)) == remote:
                    path = previous
                else:
                    length = 16
                    while True:
                        path = unique_local_path(download_dir, remote, length)
                        owner = occupied.get(_path_key(path))
                        # A file outside the manifest also owns its pathname.
                        if owner == remote or (owner is None and not os.path.lexists(path)):
                            break
                        length += 2
                        if length > 64:
                            raise RuntimeError("Cannot reserve unique local path")
                occupied[_path_key(path)] = remote
                conn.execute("UPDATE files SET local_path=? WHERE remote_path=?", (path, remote))
                assigned[remote] = path
        return assigned

    def repair_colliding_local_paths(self) -> int:
        """Requeue legacy rows sharing a case-insensitive path; retain only a verified keeper."""
        rows = self._conn().execute(
            "SELECT remote_path, local_path, size, md5, status FROM files "
            "WHERE local_path IS NOT NULL ORDER BY remote_path"
        ).fetchall()
        groups: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            key = _path_key(row["local_path"])
            groups.setdefault(key, []).append(row)
        to_requeue: list[str] = []
        for group in groups.values():
            if len(group) < 2:
                continue
            verified = []
            for row in group:
                path, size, md5 = row["local_path"], row["size"], row["md5"]
                if row["status"] not in ("downloaded", "skipped") or not md5:
                    continue
                actual_size = file_size_safe(path)
                if actual_size is None or (size is not None and actual_size != size):
                    continue
                try:
                    if compute_md5(path).lower() == md5.lower():
                        verified.append(row["remote_path"])
                except OSError:
                    pass
            keeper = min(verified) if verified else None
            to_requeue.extend(
                row["remote_path"] for row in group if row["remote_path"] != keeper
            )
        if to_requeue:
            with self._tx() as conn:
                conn.executemany(
                    "UPDATE files SET status='pending', local_path=NULL, "
                    "error='legacy local path collision', downloaded_at=NULL WHERE remote_path=?",
                    [(remote,) for remote in to_requeue],
                )
        return len(to_requeue)

    def mark_downloaded(self, remote_path: str, local_path: str) -> None:
        with self._tx() as conn:
            conn.execute(
                """
                UPDATE files
                   SET status = 'downloaded',
                       local_path = ?,
                       error = NULL,
                       downloaded_at = ?
                 WHERE remote_path = ?
                """,
                (local_path, datetime.utcnow().isoformat(timespec="seconds"), remote_path),
            )

    def mark_skipped(self, remote_path: str, local_path: str) -> None:
        with self._tx() as conn:
            conn.execute(
                """
                UPDATE files
                   SET status = 'skipped',
                       local_path = ?,
                       error = NULL,
                       downloaded_at = ?
                 WHERE remote_path = ?
                """,
                (local_path, datetime.utcnow().isoformat(timespec="seconds"), remote_path),
            )

    def mark_failed(self, remote_path: str, error: str) -> None:
        with self._tx() as conn:
            conn.execute(
                """
                UPDATE files
                   SET status = 'failed',
                       error = ?
                 WHERE remote_path = ?
                """,
                (error[:2000], remote_path),
            )

    def reset_failed_to_pending(self) -> int:
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE files SET status='pending', error=NULL WHERE status='failed'"
            )
            return cur.rowcount

    def get_pending(self) -> Iterable[sqlite3.Row]:
        """Возвращает курсор с pending-файлами. Читается лениво, без загрузки всего в RAM."""
        return self._conn().execute(
            "SELECT remote_path, size, md5, local_path FROM files WHERE status='pending' ORDER BY size ASC"
        )

    def get_failed(self) -> Iterable[sqlite3.Row]:
        return self._conn().execute(
            "SELECT remote_path, size, md5, error FROM files WHERE status='failed'"
        )

    def get_all_downloaded(self) -> Iterable[sqlite3.Row]:
        return self._conn().execute(
            "SELECT remote_path, local_path, md5, size FROM files "
            "WHERE status IN ('downloaded', 'skipped')"
        )

    def get_stats(self) -> dict[str, int]:
        rows = self._conn().execute(
            "SELECT status, COUNT(*) AS n, COALESCE(SUM(size), 0) AS bytes FROM files GROUP BY status"
        ).fetchall()
        stats: dict[str, int] = {
            "pending": 0,
            "downloaded": 0,
            "failed": 0,
            "skipped": 0,
            "total": 0,
            "bytes_total": 0,
            "bytes_downloaded": 0,
        }
        for r in rows:
            stats[r["status"]] = r["n"]
            stats["total"] += r["n"]
            stats["bytes_total"] += r["bytes"] or 0
            if r["status"] in ("downloaded", "skipped"):
                stats["bytes_downloaded"] += r["bytes"] or 0
        return stats

    def get_existing_remote_paths(self) -> set[str]:
        """Все уже зарегистрированные пути — для инкрементального scan."""
        return {
            row["remote_path"]
            for row in self._conn().execute("SELECT remote_path FROM files")
        }

    def close(self) -> None:
        """Закрывает все соединения, открытые в любых потоках."""
        with self._all_conns_lock:
            conns = list(self._all_conns)
            self._all_conns.clear()
        for conn in conns:
            try:
                conn.close()
            except sqlite3.Error:
                pass
        self._tls.conn = None
