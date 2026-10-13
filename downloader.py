"""Рекурсивный обход Яндекс.Диска и многопоточное скачивание.

Архитектура:
* scan_files() — получает плоский список всего диска, регистрирует файлы в БД.
* scan_tree() — обходит только выбранную папку.
* download_all() — берёт pending из БД и скачивает в пул потоков. Каждый поток
  имеет свой `yadisk.Client` (синхронный клиент не thread-safe для шаринга).
* Resume — локальный файл пропускается только после сверки с remote MD5.
* Корзина (`/Trash`) не трогается.
"""

from __future__ import annotations

import logging
import hashlib
import os
import sys
import threading
import time
import tempfile
from typing import Optional
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import yadisk
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)
from tqdm import tqdm

from state import State
from utils import (
    compute_md5,
    ensure_dir,
    file_size_safe,
    unique_local_path,
    to_long_path,
)

log = logging.getLogger("yadisk_backup")

# Скип-листы для путей которые НИКОГДА не качаем
SKIP_PATH_PREFIXES = ("disk:/Корзина", "disk:/Trash", "/Корзина", "/Trash")

# Какие исключения yadisk считаем "повторяемыми"
_RETRYABLE_EXC = getattr(yadisk.exceptions, "RetriableYaDiskError", None)


def rest_request_args() -> dict:
    """Read timeouts when a client is created, after .env has been loaded."""
    from runtime_settings import read_timeouts

    return {
        "timeout": read_timeouts(),
        "n_retries": 0,
    }


def _safe_error(exc: BaseException) -> str:
    """Do not persist exception text, which may contain a signed download URL."""
    if isinstance(exc, ValueError) and str(exc).startswith(
        ("size mismatch:", "MD5 mismatch")
    ):
        return str(exc)
    return type(exc).__name__


# ── retry-обёртка для сетевых вызовов ───────────────────────────────────────


# Ошибки, повторять которые бессмысленно: токен не станет валидным,
# а удалённый файл не появится, сколько ни жди.
NON_RETRYABLE = (
    yadisk.exceptions.UnauthorizedError,
    yadisk.exceptions.ForbiddenError,
    yadisk.exceptions.PathNotFoundError,
)

# Пауза при 429 — Яндекс просит подождать, экспонента с 1 секунды тут не поможет
RATE_LIMIT_WAIT_SEC = 60.0
SERVER_ERROR_WAIT_SEC = 5.0
DEFAULT_TIMEOUT = (
    30.0,
    300.0,
)  # (connect 30s, read 300s — защита от Read timed out на медленной сети и больших файлах)


def _is_retryable(exc: BaseException) -> bool:
    """Решает, стоит ли retry-ить исключение."""
    if isinstance(exc, NON_RETRYABLE):
        return False
    if isinstance(exc, yadisk.exceptions.YaDiskError) and getattr(
        exc, "disable_retry", False
    ):
        return False
    if isinstance(exc, yadisk.exceptions.TooManyRequestsError):
        return True
    if isinstance(exc, yadisk.exceptions.YaDiskError):
        return bool(_RETRYABLE_EXC and isinstance(exc, _RETRYABLE_EXC))
    if isinstance(exc, (OSError, ConnectionError, TimeoutError)):
        return True
    return False


_backoff = wait_exponential(multiplier=1, min=1, max=60)


def _wait_policy(retry_state) -> float:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, yadisk.exceptions.TooManyRequestsError):
        return RATE_LIMIT_WAIT_SEC
    if isinstance(
        exc,
        (
            yadisk.exceptions.InternalServerError,
            yadisk.exceptions.BadGatewayError,
            yadisk.exceptions.GatewayTimeoutError,
            yadisk.exceptions.UnavailableError,
        ),
    ):
        return SERVER_ERROR_WAIT_SEC
    return _backoff(retry_state)


_retry = retry(
    retry=retry_if_exception(_is_retryable),
    wait=_wait_policy,
    stop=stop_after_attempt(2),
    reraise=True,
)


# ── обход дерева ────────────────────────────────────────────────────────────


def _normalize_remote(path: str) -> str:
    """Приводит путь к виду 'disk:/foo/bar' — так возвращает API."""
    if path.startswith("disk:"):
        return path
    if path.startswith("/"):
        return "disk:" + path
    return "disk:/" + path


def _is_skipped(path: str) -> bool:
    return any(path.startswith(p) for p in SKIP_PATH_PREFIXES)


def scan_tree(
    client: yadisk.Client,
    state: State,
    root: str = "/",
    on_file: Callable[[str, int], None] | None = None,
    on_dir: Callable[[str], None] | None = None,
) -> tuple[int, int]:
    """Обходит дерево начиная с root. Регистрирует файлы как pending.
    Возвращает (количество файлов, суммарный размер).
    """
    files_count = 0
    bytes_total = 0

    # Очередь путей для обхода — итеративный BFS, не плодим рекурсию на 10 уровней
    queue: list[str] = [root]

    while queue:
        cur = queue.pop()
        if _is_skipped(_normalize_remote(cur)):
            log.info("Пропускаю корзину: %s", cur)
            continue

        try:
            entries = _list_dir(client, cur)
        except Exception as e:
            log.error("Не смог прочитать %s: %s", cur, _safe_error(e))
            raise RuntimeError(f"Обход неполон: не удалось прочитать {cur}") from e

        dir_files = []
        for entry in entries:
            entry_path = entry.path  # вид 'disk:/foo/bar'
            if _is_skipped(entry_path):
                continue

            if entry.type == "dir":
                if on_dir:
                    on_dir(entry_path)
                queue.append(entry_path)
            elif entry.type == "file":
                size = int(entry.size) if entry.size is not None else None
                md5 = getattr(entry, "md5", None)
                dir_files.append((entry_path, size, md5))
                files_count += 1
                if entry.size:
                    bytes_total += int(entry.size)
                if on_file:
                    on_file(entry_path, int(entry.size or 0))

        if dir_files:
            state.mark_pending_batch(dir_files)

    return files_count, bytes_total


@_retry
def _list_dir(client: yadisk.Client, path: str) -> list:
    """listdir с retry. Возвращает list ResourceObject."""
    # max_items=None → yadisk сам пагинирует
    return list(client.listdir(path, max_items=None))


# ── плоский список файлов через get_files ──────────────────────────────────


def scan_files(
    client: yadisk.Client,
    state: State,
    on_file: Optional[Callable[[str, int], None]] = None,
) -> tuple[int, int]:
    """Register every ordinary-disk file with one API request per page."""
    return _scan_flat_files(client, state, media_types=(None,), on_file=on_file)


def _scan_flat_files(
    client: yadisk.Client,
    state: State,
    media_types: tuple[Optional[str], ...],
    on_file: Optional[Callable[[str, int], None]],
) -> tuple[int, int]:
    files_count = 0
    bytes_total = 0
    for media_type in media_types:
        page_batch = []
        for entry in _iter_files(client, media_type=media_type):
            entry_path = entry.path
            if _is_skipped(entry_path) or entry.type != "file":
                continue
            size = int(entry.size) if entry.size is not None else None
            page_batch.append((entry_path, size, getattr(entry, "md5", None)))
            files_count += 1
            bytes_total += size or 0
            if on_file:
                on_file(entry_path, size or 0)
            if len(page_batch) >= 100:
                state.mark_pending_batch(page_batch)
                page_batch.clear()
        if page_batch:
            state.mark_pending_batch(page_batch)
    return files_count, bytes_total


def scan_media(
    client: yadisk.Client,
    state: State,
    media_types: tuple[str, ...] = ("image", "video"),
    on_file: Callable[[str, int], None] | None = None,
) -> tuple[int, int]:
    """Альтернатива scan_tree: использует client.get_files() с фильтром по типу.
    Возвращает все фото и видео со всего диска плоским списком.

    Это покрывает альбомы «Камера», «Видео», «Скриншоты» (это умные виды Яндекса
    над теми же файлами), но НЕ читает «Альбом семьи», «Люди на фото» и пр. —
    их API не отдаёт. Зато берёт все медиа из всех папок диска целиком.
    """
    return _scan_flat_files(client, state, media_types, on_file)


@_retry
def _get_files_page(
    client: yadisk.Client,
    media_type: Optional[str],
    offset: int,
    limit: int,
) -> list:
    """Одна страница из get_files с retry."""
    kwargs = {
        "offset": offset,
        "limit": limit,
        "max_items": limit,
        "sort": "path",
        "fields": ["items.path", "items.type", "items.size", "items.md5"],
    }
    if media_type is not None:
        kwargs["media_type"] = media_type
    return list(client.get_files(**kwargs))


def _iter_files(
    client: yadisk.Client, media_type: Optional[str] = None, page_size: int = 1000
):
    """Итератор по файлам диска с необязательным media_type.

    Сами пагинируем — yadisk.get_files() умеет итератор, но мы хотим
    обернуть КАЖДУЮ страницу в retry, а не одну общую попытку.
    """
    offset = 0
    seen_pages: set[bytes] = set()
    while True:
        page = _get_files_page(client, media_type, offset, page_size)
        if not page:
            return
        if len(page) > page_size:
            raise RuntimeError("Обход неполон: API вернул страницу больше запрошенной")
        digest = hashlib.sha256()
        for item in page:
            path = item.path.encode("utf-8")
            digest.update(len(path).to_bytes(4, "big"))
            digest.update(path)
        signature = digest.digest()
        if signature in seen_pages:
            raise RuntimeError("Обход неполон: API повторил страницу файлов")
        seen_pages.add(signature)
        for item in page:
            yield item
        if len(page) < page_size:
            return
        offset += page_size


# ── скачивание ──────────────────────────────────────────────────────────────


@dataclass
class DownloadJob:
    remote_path: str
    size: Optional[int]
    md5: Optional[str]
    local_path: Optional[str] = None


@dataclass
class DownloadResult:
    job: DownloadJob
    status: str  # downloaded | skipped | failed
    local_path: str | None
    bytes_written: int
    error: str | None
    duration_ms: int


def _ensure_local_dirs(local_path: str) -> None:
    parent = os.path.dirname(local_path)
    if parent:
        ensure_dir(to_long_path(parent))


def _download_with_retry(client: yadisk.Client, remote: str, local: str) -> None:
    """One bounded transfer attempt; failures remain in state for the next run."""
    try:
        os.remove(to_long_path(local))
    except FileNotFoundError:
        pass
    client.download(remote, to_long_path(local))


def _matches_expected(path: str, job: DownloadJob) -> bool:
    size = file_size_safe(path)
    if size is None or (job.size is not None and size != job.size):
        return False
    if not job.md5:
        return False
    try:
        return compute_md5(path).lower() == job.md5.lower()
    except OSError:
        return False


def _download_one(
    job: DownloadJob,
    client: yadisk.Client,
    download_dir: str,
) -> DownloadResult:
    start = time.monotonic()
    local_path = job.local_path or unique_local_path(download_dir, job.remote_path)
    try:
        _ensure_local_dirs(local_path)
    except OSError as e:
        dur = int((time.monotonic() - start) * 1000)
        return DownloadResult(job, "failed", None, 0, f"local path: {e}", dur)

    # Existing bytes are trusted only after a remote MD5 match.
    if _matches_expected(local_path, job):
        dur = int((time.monotonic() - start) * 1000)
        return DownloadResult(
            job=job,
            status="skipped",
            local_path=local_path,
            bytes_written=0,
            error=None,
            duration_ms=dur,
        )

    temp_path = None
    try:
        fd, temp_path = tempfile.mkstemp(
            prefix=".yadisk-", suffix=".part", dir=os.path.dirname(local_path)
        )
        os.close(fd)
        _download_with_retry(client, job.remote_path, temp_path)
        size = file_size_safe(temp_path)
        if size is None or (job.size is not None and size != job.size):
            raise ValueError(f"size mismatch: expected {job.size}, got {size}")
        if job.md5 and compute_md5(temp_path).lower() != job.md5.lower():
            raise ValueError("MD5 mismatch")
        os.replace(to_long_path(temp_path), to_long_path(local_path))
        temp_path = None
    except yadisk.exceptions.TooManyRequestsError:
        dur = int((time.monotonic() - start) * 1000)
        return DownloadResult(job, "failed", None, 0, "TooManyRequestsError", dur)
    except yadisk.exceptions.PathNotFoundError:
        dur = int((time.monotonic() - start) * 1000)
        return DownloadResult(job, "failed", None, 0, "PathNotFoundError", dur)
    except Exception as e:
        dur = int((time.monotonic() - start) * 1000)
        return DownloadResult(job, "failed", None, 0, _safe_error(e), dur)
    finally:
        if temp_path is not None:
            try:
                os.remove(to_long_path(temp_path))
            except OSError:
                pass

    bytes_written = file_size_safe(local_path) or 0
    dur = int((time.monotonic() - start) * 1000)
    return DownloadResult(
        job=job,
        status="downloaded",
        local_path=local_path,
        bytes_written=bytes_written,
        error=None,
        duration_ms=dur,
    )


# Каждому потоку — свой клиент. Шарить yadisk.Client между потоками небезопасно.
_thread_local = threading.local()
# Реестр всех созданных клиентов — чтобы закрыть сокеты после остановки пула
_all_clients: list[yadisk.Client] = []
_all_clients_lock = threading.Lock()


def _get_thread_client(token: str) -> yadisk.Client:
    cli = getattr(_thread_local, "client", None)
    if cli is None:
        # YaDisk defaults can retry internally; keep one attempt per file/run.
        cli = yadisk.Client(token=token, default_args=rest_request_args())
        _thread_local.client = cli
        with _all_clients_lock:
            _all_clients.append(cli)
    return cli


def _close_all_clients() -> None:
    """Закрывает клиентов всех воркеров. Вызывать ПОСЛЕ остановки пула —
    потоки уже мертвы, их thread-local исчез вместе с ними.
    """
    with _all_clients_lock:
        clients = list(_all_clients)
        _all_clients.clear()
    for cli in clients:
        try:
            cli.close()
        except Exception:  # noqa: BLE001 — закрытие не должно ронять выход
            pass


def download_all(
    token: str,
    state: State,
    download_dir: str,
    workers: int = 4,
    jobs: Iterator[DownloadJob] | None = None,
) -> dict[str, int]:
    """Скачивает все pending файлы из state в пул потоков.
    Возвращает счётчики: {downloaded, skipped, failed, bytes}.
    """
    workers = max(workers, 1)
    if workers > 5:
        log.warning(
            "workers=%d > 5 — Яндекс будет резать по rate limit. Снижаю до 5.", workers
        )
        workers = 5

    download_dir = os.path.abspath(download_dir)
    ensure_dir(download_dir)
    repaired = state.repair_colliding_local_paths()
    if repaired:
        log.warning(
            "Повторно поставлено в очередь из-за старых коллизий путей: %d", repaired
        )

    # Собираем список jobs из БД (или из переданного итератора — для retry-failed)
    if jobs is None:
        jobs_list = [
            DownloadJob(
                remote_path=row["remote_path"],
                size=row["size"],
                md5=row["md5"],
                local_path=row["local_path"],
            )
            for row in state.get_pending()
        ]
    else:
        jobs_list = list(jobs)

    if jobs_list:
        paths = state.reserve_local_paths(
            download_dir, [j.remote_path for j in jobs_list]
        )
        for job in jobs_list:
            job.local_path = paths[job.remote_path]

    if not jobs_list:
        log.info("Нет файлов для скачивания.")
        return {"downloaded": 0, "skipped": 0, "failed": 0, "bytes": 0}

    total_bytes = sum(j.size for j in jobs_list if j.size)
    counters = {"downloaded": 0, "skipped": 0, "failed": 0, "bytes": 0}

    file_bar = tqdm(total=len(jobs_list), unit="file", desc="Файлы", position=0)
    byte_bar = tqdm(
        total=total_bytes,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc="Байты",
        position=1,
    )

    def worker(job: DownloadJob) -> DownloadResult:
        try:
            client = _get_thread_client(token)
            return _download_one(job, client, download_dir)
        except Exception as e:
            return DownloadResult(job, "failed", None, 0, _safe_error(e), 0)

    # Без `with`: его выход дожидается ВСЕЙ очереди, и Ctrl+C выглядит зависанием.
    pool = ThreadPoolExecutor(max_workers=workers)
    try:
        futures = {pool.submit(worker, j): j for j in jobs_list}
        try:
            for fut in as_completed(futures):
                res = fut.result()
                _apply_result(state, res, counters, file_bar, byte_bar)
        except KeyboardInterrupt:
            file_bar.close()
            byte_bar.close()
            print(
                "\nCtrl+C — отменяю очередь, дожидаюсь только текущих файлов...",
                file=sys.stderr,
            )
            log.warning("Ctrl+C — отменяю оставшиеся задачи и закрываю пул...")
            pool.shutdown(wait=False, cancel_futures=True)
            raise
        pool.shutdown(wait=True)
        # Закрываем сокеты только когда воркеры точно остановились
        _close_all_clients()
    finally:
        file_bar.close()
        byte_bar.close()

    return counters


def _apply_result(
    state: State,
    res: DownloadResult,
    counters: dict[str, int],
    file_bar: tqdm,
    byte_bar: tqdm,
) -> None:
    """Применяет результат скачивания к БД и счётчикам."""
    if res.status == "downloaded":
        state.mark_downloaded(res.job.remote_path, res.local_path or "")
        counters["downloaded"] += 1
        counters["bytes"] += res.bytes_written
        byte_bar.update(res.bytes_written)
        log.info(
            "OK | %s | %d B | %d ms | %s",
            res.status,
            res.bytes_written,
            res.duration_ms,
            res.job.remote_path,
        )
    elif res.status == "skipped":
        state.mark_skipped(res.job.remote_path, res.local_path or "")
        counters["skipped"] += 1
        if res.job.size:
            byte_bar.update(res.job.size)
        log.info(
            "SKIP | already exists | %s",
            res.job.remote_path,
        )
    else:
        state.mark_failed(res.job.remote_path, res.error or "unknown")
        counters["failed"] += 1
        log.error(
            "FAIL | %s | %s | %s",
            res.error,
            res.duration_ms,
            res.job.remote_path,
        )
    file_bar.update(1)


# ── проверка токена ─────────────────────────────────────────────────────────


def check_token(token: str) -> tuple[bool, str]:
    """Проверяет, валиден ли токен. Возвращает (ok, message)."""
    if not token or token.strip() == "" or token.startswith("AQAAA_paste"):
        return False, (
            "Токен не задан. Открой https://yandex.ru/dev/disk/poligon/, "
            "получи OAuth-токен по кнопке справа вверху и положи его в .env "
            "как YA_DISK_TOKEN=..."
        )
    try:
        cli = yadisk.Client(token=token, default_args=rest_request_args())
        if not cli.check_token():
            return False, (
                "Токен невалиден (Яндекс ответил, что он не работает). "
                "Получи новый на https://yandex.ru/dev/disk/poligon/"
            )
        info = cli.get_disk_info()
        used = getattr(info, "used_space", 0) or 0
        total = getattr(info, "total_space", 0) or 0
        cli.close()
        return (
            True,
            f"Токен OK. Использовано {used / 1e9:.2f} ГБ из {total / 1e9:.2f} ГБ.",
        )
    except yadisk.exceptions.UnauthorizedError:
        return False, (
            "Токен не принят (401 Unauthorized). Получи новый на "
            "https://yandex.ru/dev/disk/poligon/"
        )
    except Exception as e:
        return False, f"Не удалось проверить токен: {type(e).__name__}: {e}"
