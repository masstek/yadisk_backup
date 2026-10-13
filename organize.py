"""Сортирует папку с бэкапом по полочкам.

Что делает:
  1. Сканирует папку рекурсивно
  2. Дедуп по (size, md5) — дубликаты → _duplicates/
  3. Раскладывает по типам:
     - Фото:    Фото/YYYY/YYYY-MM/
     - Видео:   Видео/YYYY/YYYY-MM/
     - Док:     Документы/
     - Архивы:  Архивы/
     - Программы: Программы/
     - Аудио:   Аудио/
     - Прочее:  Прочее/

  Дату берёт из имени файла (YYYY-MM-DD), иначе из mtime.

Запуск:
  python organize.py --input "E:/папка" --action report-only
  python organize.py --input "E:/папка" --action move
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sqlite3
import sys
import time
from collections import Counter, defaultdict

from utils import (
    DIR_ARCHIVE,
    DIR_AUDIO,
    DIR_DOC,
    DIR_DUP,
    DIR_OTHER,
    DIR_PHOTO,
    DIR_PROGRAM,
    DIR_VIDEO,
    classify_dir,
    human_bytes,
)
from utils import (
    compute_md5 as compute_md5_file,
)

SKIP_DIRS = {
    DIR_PHOTO,
    DIR_VIDEO,
    DIR_DOC,
    DIR_ARCHIVE,
    DIR_PROGRAM,
    DIR_AUDIO,
    DIR_OTHER,
    DIR_DUP,
    ".sync",
}

DATE_REGEXES = [
    re.compile(r"(20\d{2})[-_.](\d{2})[-_.](\d{2})"),
    re.compile(r"(20\d{2})(\d{2})(\d{2})"),
    re.compile(r"IMG_(20\d{2})(\d{2})(\d{2})"),
    re.compile(r"VID-(20\d{2})(\d{2})(\d{2})"),
]


def get_date(path: str) -> tuple[int, int]:
    name = os.path.basename(path)
    for rx in DATE_REGEXES:
        m = rx.search(name)
        if m:
            try:
                y = int(m.group(1))
                mo = int(m.group(2))
                if 2000 <= y <= 2030 and 1 <= mo <= 12:
                    return y, mo
            except (ValueError, IndexError):
                pass
    try:
        t = time.localtime(os.path.getmtime(path))
        return t.tm_year, t.tm_mon
    except OSError:
        return 1970, 1


def target_path(root: str, src: str, category: str) -> str:
    name = os.path.basename(src)
    if category in (DIR_PHOTO, DIR_VIDEO):
        y, mo = get_date(src)
        sub = os.path.join(root, category, str(y), f"{y}-{mo:02d}")
    else:
        sub = os.path.join(root, category)
    return os.path.join(sub, name)


def unique_path(dst: str) -> str:
    if not os.path.exists(dst):
        return dst
    base, ext = os.path.splitext(dst)
    i = 1
    while True:
        cand = f"{base}_{i}{ext}"
        if not os.path.exists(cand):
            return cand
        i += 1


def scan(root: str) -> list[dict]:
    files = []
    skip = {os.path.join(root, d) for d in SKIP_DIRS}
    print(f"\n[1/4] Сканирую {root}...")
    for dp, dirs, fns in os.walk(root):
        dirs[:] = [d for d in dirs if os.path.join(dp, d) not in skip]
        for fn in fns:
            full = os.path.join(dp, fn)
            try:
                files.append({"path": full, "size": os.path.getsize(full)})
            except OSError:
                pass
    print(f"  Найдено: {len(files)}")
    return files


def open_manifests(state_db: str, unlim_state_db: str):
    """Open existing manifests through one connection for coordinated updates."""
    sources = [(state_db, "files"), (unlim_state_db, "unlim_files")]
    existing = [
        (os.path.abspath(path), table)
        for path, table in sources
        if os.path.isfile(path)
    ]
    if not existing:
        return None, []
    conn = sqlite3.connect(existing[0][0], timeout=30, isolation_level=None)
    updates = []
    aliases = {existing[0][0]: "main"}
    try:
        for path, table in existing:
            alias = aliases.get(path)
            if alias is None:
                alias = f"manifest_{len(aliases)}"
                conn.execute(f"ATTACH DATABASE ? AS {alias}", (path,))
                aliases[path] = alias
            found = conn.execute(
                f"SELECT 1 FROM {alias}.sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not found:
                raise RuntimeError(f"В {path} нет таблицы {table}")
            updates.append(
                f"UPDATE {alias}.{table} SET local_path=? WHERE local_path=?"
            )
        return conn, updates
    except Exception:
        conn.close()
        raise


def compute_md5_batch(files: list[dict]):
    print("\n[2/4] MD5 для подозреваемых...")
    by_size = defaultdict(list)
    for f in files:
        by_size[f["size"]].append(f)
    suspect = [f for v in by_size.values() if len(v) > 1 for f in v]
    print(f"  Подозреваемых: {len(suspect)}")
    for i, f in enumerate(suspect):
        try:
            f["md5"] = compute_md5_file(f["path"])
        except OSError:
            f["md5"] = None
        if (i + 1) % 500 == 0:
            print(f"    progress: {i + 1}/{len(suspect)}")


def find_dup_paths(files: list[dict]) -> set[str]:
    g = defaultdict(list)
    for f in files:
        if f.get("md5"):
            g[(f["size"], f["md5"])].append(f)
    out = set()
    for grp in g.values():
        if len(grp) > 1:
            grp.sort(key=lambda x: (len(x["path"]), x["path"]))
            for d in grp[1:]:
                out.add(d["path"])
    return out


def main():
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--action", default="report-only", choices=["report-only", "move"])
    ap.add_argument(
        "--state-db",
        default=os.path.join(os.path.dirname(__file__), "state.db"),
        help="SQLite manifest whose local_path is updated after each move",
    )
    ap.add_argument(
        "--unlim-state-db",
        default=os.path.join(os.path.dirname(__file__), "unlim_state.db"),
        help="Photoslice manifest whose local_path is updated after each move",
    )
    args = ap.parse_args()

    root = os.path.abspath(args.input)
    if not os.path.isdir(root):
        sys.exit(f"Нет папки: {root}")

    print("=" * 60)
    print(f"  СОРТИРОВКА: {root}")
    print(f"  Действие: {args.action}")
    print("=" * 60)

    t0 = time.time()
    files = scan(root)
    compute_md5_batch(files)
    dups = find_dup_paths(files)

    print("\n[3/4] Раскладываю...")
    plan = []
    cat_n = Counter()
    cat_b = Counter()
    dup_b = 0
    for f in files:
        src = f["path"]
        if src in dups:
            rel = os.path.relpath(src, root)
            plan.append((src, os.path.join(root, DIR_DUP, rel), True))
            dup_b += f["size"]
        else:
            cat = classify_dir(src)
            plan.append((src, target_path(root, src, cat), False))
            cat_n[cat] += 1
            cat_b[cat] += f["size"]

    total_b = sum(f["size"] for f in files)
    print("\n[4/4] СТАТИСТИКА:")
    print(f"  Всего:           {len(files)} ({human_bytes(total_b)})")
    print(f"  Дубликатов:      {len(dups)} ({human_bytes(dup_b)})")
    print(f"  Уникальных:      {len(files) - len(dups)}")
    print("\n  По полкам:")
    for cat in [
        DIR_PHOTO,
        DIR_VIDEO,
        DIR_AUDIO,
        DIR_DOC,
        DIR_ARCHIVE,
        DIR_PROGRAM,
        DIR_OTHER,
    ]:
        if cat_n[cat]:
            print(f"    {cat:<12} {cat_n[cat]:>6} файлов ({human_bytes(cat_b[cat])})")
    print(f"\n  Скан: {time.time() - t0:.0f} сек")

    if args.action == "report-only":
        print("\n[INFO] Отчёт. Запусти --action move чтобы реально сделать.")
        return 0

    conn, manifest_updates = open_manifests(args.state_db, args.unlim_state_db)

    print(f"\nПеремещаю {len(plan)}...")
    moved = 0
    err = 0
    last = time.time()
    for src, dst, is_dup in plan:
        if os.path.abspath(src) == os.path.abspath(dst):
            continue
        actual_dst = None
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            actual_dst = unique_path(dst)
            if conn is not None:
                conn.execute("BEGIN IMMEDIATE")
            shutil.move(src, actual_dst)
            if conn is not None:
                for update in manifest_updates:
                    conn.execute(update, (actual_dst, src))
                conn.execute("COMMIT")
            moved += 1
        except Exception as e:
            if conn is not None and conn.in_transaction:
                conn.execute("ROLLBACK")
            if actual_dst and os.path.isfile(actual_dst) and not os.path.exists(src):
                try:
                    shutil.move(actual_dst, src)
                except OSError:
                    pass
            err += 1
            if err <= 5:
                print(f"  err: {src}: {e}")
        if time.time() - last > 5:
            print(f"  progress: {moved}/{len(plan)} (err={err})")
            last = time.time()

    cleaned = 0
    for dp, dirs, fns in os.walk(root, topdown=False):
        rel = os.path.relpath(dp, root)
        first = rel.split(os.sep)[0] if rel != "." else ""
        if first in SKIP_DIRS:
            continue
        try:
            if not os.listdir(dp) and dp != root:
                os.rmdir(dp)
                cleaned += 1
        except OSError:
            pass

    print("\n✅ ГОТОВО:")
    print(f"  Перемещено: {moved}")
    print(f"  Ошибок:     {err}")
    print(f"  Удалено пустых папок: {cleaned}")
    if conn is not None:
        conn.close()
    return 1 if err else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
