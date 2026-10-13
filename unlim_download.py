"""Сборка списка файлов безлимитного фото-хранилища (photoslice) через web API.

Использует cookies.json (экспортированные из браузера расширением Cookie-Editor).
Запоминает все файлы в unlim_state.db со статусом 'pending'. Само скачивание —
в unlim_worker.py (можно запускать несколько параллельно).

CLI:
  python unlim_download.py --collect    # собрать список из photoslice
  python unlim_download.py --status     # показать статус БД
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import logging
import os
import sqlite3
import sys
import threading
import time


from unlim_api import (
    DownloadError,
    api_call,
    get_sk_uid as _get_sk_uid,
    make_session as _make_session,
    resource_path,
    Timeouts,
)
from unlim_state import ensure_schema
from dotenv import load_dotenv
from tqdm import tqdm

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT, ".env"))
COOKIES_FILE = os.path.join(PROJECT, "cookies.json")
DB_FILE = os.path.join(PROJECT, "unlim_state.db")
LOG_FILE = os.path.join(PROJECT, "logs", "unlim.log")

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    encoding="utf-8",
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("unlim")


# ── DB ──────────────────────────────────────────────────────────────────────

_tls = threading.local()


def db() -> sqlite3.Connection:
    c = getattr(_tls, "c", None)
    if c is None:
        c = sqlite3.connect(
            DB_FILE, timeout=30, isolation_level=None, check_same_thread=False
        )
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
        c.row_factory = sqlite3.Row
        _tls.c = c
    return c


def db_init():
    ensure_schema(DB_FILE)


# ── HTTP ────────────────────────────────────────────────────────────────────


def make_session():
    return _make_session(COOKIES_FILE)


def get_sk_uid(s):
    return _get_sk_uid(s, Timeouts.from_env())


# ── Логика сбора photoslice ─────────────────────────────────────────────────


def init_snapshot(s, sk, uid) -> str:
    d = api_call(s, sk, uid, "intapi/photo-init-snapshot")
    psid = d.get("photoslice_id")
    if not psid:
        raise DownloadError("photo-init-snapshot: нет photoslice_id", fatal=True)
    return psid


@dataclass
class ClusterScanResult:
    items: list[dict]
    complete: bool
    warning: str | None = None


def get_all_clusters(s, sk, uid, psid) -> ClusterScanResult:
    items, seen, offset = [], set(), 0
    amount = 1000
    while True:
        try:
            page = api_call(
                s,
                sk,
                uid,
                "intapi/photo-get-clusters",
                {"id": psid, "amount": amount, "offset": offset},
            )
        except DownloadError as e:
            if e.fatal:
                raise
            return ClusterScanResult(
                items, False, f"photo-get-clusters: ошибка запроса при offset={offset}"
            )
        batch = page.get("items") if isinstance(page, dict) else None
        if not isinstance(batch, list):
            return ClusterScanResult(
                items,
                False,
                f"photo-get-clusters: нет списка items при offset={offset}",
            )
        if not batch:
            print(
                f"  photo-get-clusters: offset={offset}, получено=0/{amount}, новых=0, всего={len(items)}"
            )
            return ClusterScanResult(items, True)
        added = 0
        for item in batch:
            if not isinstance(item, dict):
                return ClusterScanResult(
                    items,
                    False,
                    f"photo-get-clusters: неверный элемент при offset={offset}",
                )
            cid = item.get("cluster_id") or item.get("id")
            if not isinstance(cid, str) or not cid:
                return ClusterScanResult(
                    items,
                    False,
                    f"photo-get-clusters: нет cluster_id при offset={offset}",
                )
            if cid not in seen:
                normalized = {**item, "id": cid}
                if isinstance(item.get("items"), list):
                    normalized["size"] = len(item["items"])
                items.append(normalized)
                seen.add(cid)
                added += 1
        print(
            f"  photo-get-clusters: offset={offset}, получено={len(batch)}/{amount}, "
            f"новых={added}, всего={len(items)}"
        )
        if (
            offset == 0
            and len(batch) > amount
            and all(isinstance(c.get("items"), list) for c in items)
        ):
            # Some API variants return the whole index despite amount/offset. Confirm
            # its cluster IDs against the independently exposed snapshot index.
            try:
                snapshot = api_call(
                    s,
                    sk,
                    uid,
                    "intapi/photo-get-snapshot",
                    {"id": psid, "locale": "ru", "amount": 100000, "offset": 0},
                )
            except DownloadError as e:
                if e.fatal:
                    raise
                return ClusterScanResult(
                    items, False, "photo-get-snapshot: ошибка сверки списка кластеров"
                )
            snapshot_items = (
                snapshot.get("items") if isinstance(snapshot, dict) else None
            )
            snapshot_ids = (
                [c.get("id") for c in snapshot_items]
                if isinstance(snapshot_items, list)
                and all(
                    isinstance(c, dict) and isinstance(c.get("id"), str) and c["id"]
                    for c in snapshot_items
                )
                else []
            )
            snapshot_sizes = (
                {c["id"]: c.get("size") for c in snapshot_items} if snapshot_ids else {}
            )
            sizes_match = len(snapshot_sizes) == len(items) and all(
                type(snapshot_sizes.get(c["id"])) is int
                and snapshot_sizes[c["id"]] == c["size"]
                for c in items
            )
            if (
                len(snapshot_ids) < 100000
                and len(snapshot_ids) == len(items)
                and len(set(snapshot_ids)) == len(items)
                and set(snapshot_ids) == seen
                and sizes_match
            ):
                print(
                    f"  photo-get-snapshot: сверено кластеров={len(items)}, "
                    f"файлов={sum(c['size'] for c in items)}"
                )
                return ClusterScanResult(items, True)
            return ClusterScanResult(
                items,
                False,
                f"photo-get-snapshot: список кластеров или размеры не совпали; "
                f"получено={len(snapshot_ids)}, уникальных={len(items)}",
            )
        if len(batch) < amount:
            return ClusterScanResult(items, True)
        if not added:
            return ClusterScanResult(
                items,
                False,
                f"photo-get-clusters: повтор полной страницы при offset={offset}",
            )
        offset += len(batch)


def save_clusters(items: list[dict]):
    c = db()
    c.execute("BEGIN IMMEDIATE")
    try:
        for it in items:
            c.execute(
                "INSERT INTO unlim_clusters (cluster_id, size) VALUES (?, ?) "
                "ON CONFLICT(cluster_id) DO UPDATE SET size=excluded.size, fetched=0",
                (it.get("id"), int(it.get("size", 0) or 0)),
            )
        c.execute("COMMIT")
    except Exception:
        try:
            c.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise


def _resource_records(response, cid, offset, limit):
    """Support full metadata in resources.fetched and legacy inline cluster items."""
    resources = (response.get("resources") or {}).get("fetched") or []
    aliases = {}
    records = []
    for item in resources:
        if not isinstance(item, dict):
            raise DownloadError("collect: неверный формат resource", fatal=True)
        records.append(item)
        for field in ("path", "id", "resource_id"):
            if item.get(field):
                aliases[item[field]] = item
    clusters = (response.get("clusters") or {}).get("fetched") or []
    for cluster in clusters:
        if (cluster.get("id") or cluster.get("cluster_id")) != cid:
            continue
        refs = cluster.get("items") or []
        if len(refs) > limit:
            refs = refs[offset : offset + limit]
        for item in refs:
            if isinstance(item, str):
                records.append(aliases.get(item) or {"path": item})
            elif isinstance(item, dict):
                key = item.get("path") or item.get("id") or item.get("resource_id")
                records.append({**item, **aliases.get(key, {})})
    for item in (response.get("resources") or {}).get("missing") or []:
        if isinstance(item, str):
            records.append({"path": item})
        elif isinstance(item, dict):
            records.append(item)
    unique = {}
    for item in records:
        key = item.get("path") or item.get("id") or item.get("resource_id")
        if key:
            unique[key] = {**unique.get(key, {}), **item}
    return list(unique.values())


def _save_resources(records, cid):
    c = db()
    c.execute("BEGIN IMMEDIATE")
    added = 0
    try:
        for f in records:
            raw_path = f.get("path") or f.get("id") or f.get("resource_id")
            if not raw_path:
                raise DownloadError("collect: ресурс без идентификатора", fatal=True)
            try:
                path = resource_path(raw_path)
            except DownloadError:
                path = None  # Retain an unavailable reference for a future rescan.
            fid = raw_path
            if path and path.startswith("/photounlim/"):
                fid = "photounlim:" + path[len("/photounlim") :]
            elif path and path.startswith("/disk/"):
                fid = "disk:" + path[len("/disk") :]
            # Preserve an existing row even if an older collector used another alias.
            for alias in (fid, f.get("id"), f.get("resource_id"), raw_path):
                if (
                    alias
                    and c.execute(
                        "SELECT 1 FROM unlim_files WHERE file_id=?", (alias,)
                    ).fetchone()
                ):
                    fid = alias
                    break
            meta = f.get("meta") or {}
            size = f.get("size", meta.get("size"))
            size = int(size) if size is not None else None
            md5 = f.get("md5") or meta.get("md5")
            name = f.get("name") or str(raw_path).rsplit("/", 1)[-1]
            mime = f.get("mimetype") or meta.get("mimetype") or meta.get("mediatype")
            existed = c.execute(
                "SELECT 1 FROM unlim_files WHERE file_id=?", (fid,)
            ).fetchone()
            c.execute(
                """INSERT INTO unlim_files(file_id,cluster_id,name,size,md5,mime,download_path)
                         VALUES (?,?,?,?,?,?,?) ON CONFLICT(file_id) DO UPDATE SET
                         cluster_id=excluded.cluster_id, name=excluded.name,
                         download_path=COALESCE(excluded.download_path,unlim_files.download_path),
                         status=CASE WHEN unlim_files.status='downloaded' AND (
                           (excluded.size IS NOT NULL AND excluded.size IS NOT unlim_files.size) OR
                           (excluded.md5 IS NOT NULL AND excluded.md5 IS NOT unlim_files.md5))
                           THEN 'pending' ELSE unlim_files.status END,
                         size=COALESCE(excluded.size,unlim_files.size),
                         md5=COALESCE(excluded.md5,unlim_files.md5), mime=COALESCE(excluded.mime,unlim_files.mime)
                         WHERE unlim_files.status!='in_progress'""",
                (fid, cid, name, size, md5, mime, path),
            )
            added += int(not existed)
        c.execute("COMMIT")
    except BaseException:
        c.execute("ROLLBACK")
        raise
    return added


def _inline_cluster_records(cluster):
    refs = cluster["items"]
    records = []
    for ref in refs:
        if isinstance(ref, str):
            ref = {"path": ref}
        if (
            not isinstance(ref, dict)
            or not isinstance(ref.get("path"), str)
            or not ref["path"]
        ):
            raise DownloadError("collect: кластер содержит ресурс без пути")
        records.append(ref)
    return records


def fetch_cluster_resources(s, sk, uid, psid, cluster_rows: list[dict]) -> int:
    added = 0
    for row in cluster_rows:
        cid = row["cluster_id"]
        expected = int(row["size"] or 0)
        offset, seen = 0, set()
        while True:
            limit = min(200, expected - offset) if expected else 200
            if limit <= 0:
                break
            res = api_call(
                s,
                sk,
                uid,
                "intapi/photo-get-clusters-with-resources",
                {
                    "photosliceId": psid,
                    "clusters": {cid: {"range": [offset, offset + limit - 1]}},
                    "hideScreenshots": False,
                },
            )
            records = _resource_records(res, cid, offset, limit)
            keys = {
                r.get("path") or r.get("id") or r.get("resource_id") for r in records
            }
            fresh = keys - seen
            if not fresh:
                if expected and len(seen) < expected:
                    raise DownloadError("collect: кластер получен не полностью")
                if records:
                    raise DownloadError("collect: повтор страницы ресурсов")
                break
            added += _save_resources(records, cid)
            seen.update(fresh)
            offset += len(fresh)
            if expected and len(seen) >= expected:
                break
        db().execute("UPDATE unlim_clusters SET fetched=1 WHERE cluster_id=?", (cid,))
    return added


# ── CLI ─────────────────────────────────────────────────────────────────────


def cmd_collect():
    started_at = time.monotonic()
    db_init()
    s = make_session()
    try:
        sk, uid = get_sk_uid(s)
        print(f"  ✓ uid={uid}, sk OK")

        psid = init_snapshot(s, sk, uid)
        print("  ✓ photoslice_id получен")

        scan = get_all_clusters(s, sk, uid, psid)
        clusters = scan.items
        print(f"  ✓ кластеров (дней с фото): {len(clusters)}")
        if scan.warning:
            print(f"  ⚠ {scan.warning}; список кластеров может быть неполным")
            log.warning("%s; найдено кластеров=%s", scan.warning, len(clusters))
        save_clusters(clusters)

        todo = [{"cluster_id": c["id"], "size": c.get("size", 0)} for c in clusters]
        inline_clusters = sum(isinstance(c.get("items"), list) for c in clusters)
        metadata_clusters = len(clusters) - inline_clusters
        print(
            f"\nСохраняю список из общего индекса: {inline_clusters} кластеров уже содержат файлы; "
            f"для {metadata_clusters} кластеров запрашиваю состав отдельно..."
        )
        BATCH = 1
        bar = tqdm(total=len(todo), unit="cluster")
        added_total = 0
        errors = 0
        try:
            for i in range(0, len(todo), BATCH):
                batch = todo[i : i + BATCH]
                try:
                    cluster = clusters[i]
                    if isinstance(cluster.get("items"), list):
                        n = _save_resources(
                            _inline_cluster_records(cluster), cluster["id"]
                        )
                        db().execute(
                            "UPDATE unlim_clusters SET fetched=1 WHERE cluster_id=?",
                            (cluster["id"],),
                        )
                    else:
                        n = fetch_cluster_resources(s, sk, uid, psid, batch)
                    added_total += n
                except DownloadError as e:
                    if e.fatal:
                        raise
                    errors += 1
                    log.error("collect batch %s: %s", i, e)
                except Exception as e:
                    errors += 1
                    log.error("collect batch %s: %s", i, type(e).__name__)
                bar.update(len(batch))
                bar.set_postfix(files=added_total)
        finally:
            bar.close()

        stats = {
            r["status"]: r["n"]
            for r in db().execute(
                "SELECT status, COUNT(*) AS n FROM unlim_files GROUP BY status"
            )
        }
        elapsed = round(time.monotonic() - started_at, 1)
        print(f"\n  ✓ Файлов в БД: {sum(stats.values())} (сбор: {elapsed} с)")
        for k, v in stats.items():
            print(f"     {k}: {v}")
        if errors or not scan.complete:
            print(
                f"  Сбор неполный: ошибок кластеров={int(not scan.complete)}, "
                f"ошибок ресурсов={errors}. Повторите --collect."
            )
            return 1
        return 0
    except DownloadError as e:
        print(f"Ошибка: {e}")
        return 2 if e.fatal else 1
    finally:
        try:
            s.close()
        except Exception:
            pass


def cmd_status():
    if not os.path.exists(DB_FILE):
        print("БД пуста — запусти --collect")
        return
    db_init()
    stats = {
        r["status"]: r["n"]
        for r in db().execute(
            "SELECT status, COUNT(*) AS n FROM unlim_files GROUP BY status"
        )
    }
    total = sum(stats.values())
    print(f"Всего файлов в безлимите: {total}")
    for k, v in stats.items():
        print(f"  {k}: {v}")


def cmd_retry_failed():
    """Возвращает failed-файлы в очередь (сбрасывает счётчик попыток)."""
    if not os.path.exists(DB_FILE):
        print("БД пуста — запусти --collect")
        return
    db_init()
    from unlim_state import retry_failed

    n = retry_failed(DB_FILE)
    print(f"Возвращено в очередь: {n}")


def main():
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--collect", action="store_true", help="Собрать список файлов из photoslice"
    )
    ap.add_argument("--status", action="store_true", help="Показать статус БД")
    ap.add_argument(
        "--retry-failed",
        action="store_true",
        help="Вернуть упавшие и зависшие файлы в очередь",
    )
    args = ap.parse_args()
    if args.collect:
        return cmd_collect()
    elif args.status:
        return cmd_status()
    elif args.retry_failed:
        return cmd_retry_failed()
    else:
        ap.print_help()


if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except DownloadError as e:
        print(f"Ошибка: {e}")
        sys.exit(2 if e.fatal else 1)
    except KeyboardInterrupt:
        sys.exit(130)
