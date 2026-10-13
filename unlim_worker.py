"""Download Photoslice originals individually. No ZIP/bulk endpoint.

Timeouts may be retried while the worker owns a file. Other unavailable files
are saved as failed and retried by the next run. Give parallel CLI workers the
same --run-id (UI does this).
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import hashlib
import logging
import os
import re
from pathlib import Path
import signal
import sys
import threading
import time
import uuid

import requests
from dotenv import load_dotenv

from unlim_api import (DownloadError, Timeouts, check_http, get_sk_uid,
                       is_request_timeout, make_session, original_info,
                       validate_download_url)
import unlim_state as queue
from utils import sanitize_component

PROJECT = Path(__file__).resolve().parent
DB_FILE = str(PROJECT / 'unlim_state.db')
COOKIES_FILE = str(PROJECT / 'cookies.json')
log = logging.getLogger('unlim_worker')


class SkipReporter:
    """Bound repetitive HTTP 404 console output; other failures print immediately."""
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.first = True
        self.pending = Counter()
        self.last_report = time.monotonic()

    def record(self, file_id, code):
        if self.first:
            key = hashlib.sha256(file_id.encode()).hexdigest()[:12]
            print(f'[w{self.worker_id}] пропуск file_key={key}: {code}; сохранено для следующего запуска', flush=True)
            self.first = False
            self.last_report = time.monotonic()
            return
        self.pending[code] += 1
        if sum(self.pending.values()) >= 100 or time.monotonic() - self.last_report >= 10:
            self.flush()

    def flush(self):
        if not self.pending:
            return
        total = sum(self.pending.values())
        common = self.pending.most_common(3)
        details = ', '.join(f'{code} ×{count}' for code, count in common)
        others = total - sum(count for _, count in common)
        if others:
            details += f', другие ×{others}'
        print(f'[w{self.worker_id}] пропуски: {total} ({details}); детали в БД/логе', flush=True)
        self.pending.clear()
        self.last_report = time.monotonic()


def file_destination(root, job):
    # A directory per immutable remote ID prevents name, case and sanitization collisions.
    key = hashlib.sha256(job['file_id'].encode('utf-8')).hexdigest()
    name = str(job.get('name') or job['file_id'].rsplit('/', 1)[-1]).replace('\\', '/').rsplit('/', 1)[-1]
    name = sanitize_component(name)
    # Limit Unicode byte length, preserving a useful extension.
    stem, ext = os.path.splitext(name)
    name = stem.encode('utf-8')[:120].decode('utf-8', errors='ignore') + ext[:16]
    return str(Path(root) / key[:2] / key / (name or 'original'))


def hash_file(path):
    md5, sha = hashlib.md5(), hashlib.sha256()
    size = 0
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(256 * 1024), b''):
            size += len(chunk)
            md5.update(chunk)
            sha.update(chunk)
    return size, md5.hexdigest(), sha.hexdigest()


def verified_existing(dst, expected_size, expected_md5):
    # Size alone cannot distinguish two different originals or a corrupt previous copy.
    if not expected_md5 or not os.path.isfile(dst):
        return None
    size, md5, sha = hash_file(dst)
    if (expected_size is None or size == expected_size) and md5 == expected_md5:
        return size, sha
    return None


@contextmanager
def heartbeat(db_path, job):
    stop = threading.Event()
    lost = threading.Event()
    def run():
        while not stop.wait(20):
            try:
                if not queue.renew(db_path, job):
                    lost.set()
                    return
            except Exception:
                lost.set()
                return
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield lost
    finally:
        stop.set()
        thread.join(timeout=2)


def download_file(s, url, tmp, expected_size, expected_md5, timeouts, lost):
    """Bound connection/inactivity, not total transfer length (large videos are valid)."""
    url = validate_download_url(url)
    try:
        # Follow a small number of redirects explicitly; prohibit HTTPS downgrade.
        for _ in range(4):
            response = s.get(url, stream=True, timeout=timeouts.pair, allow_redirects=False,
                             headers={'Accept-Encoding': 'identity'})
            if response.status_code in (301, 302, 303, 307, 308):
                from urllib.parse import urljoin
                location = response.headers.get('Location', '')
                response.close()
                url = validate_download_url(urljoin(url, location))
                continue
            break
        else:
            raise DownloadError('file: слишком много перенаправлений')
        with response as r:
            check_http(r, 'file')
            content_type = r.headers.get('Content-Type', '').split(';')[0].lower()
            if content_type in ('text/html', 'application/json'):
                raise DownloadError('file: сервер вернул страницу/ошибку вместо оригинала')
            length = r.headers.get('Content-Length')
            try:
                length = int(length) if length is not None else None
            except ValueError:
                raise DownloadError('file: некорректный Content-Length') from None
            if expected_size is not None and length is not None and length != expected_size:
                raise DownloadError('file: Content-Length не совпадает с размером оригинала')
            size, md5, sha = 0, hashlib.md5(), hashlib.sha256()
            with open(tmp, 'xb') as f:
                for chunk in r.iter_content(256 * 1024):
                    if lost.is_set():
                        raise DownloadError('file: потеряно владение заданием', fatal=True)
                    if chunk:
                        f.write(chunk)
                        size += len(chunk)
                        md5.update(chunk)
                        sha.update(chunk)
                        if expected_size is not None and size > expected_size:
                            raise DownloadError('file: ответ больше ожидаемого оригинала')
                f.flush()
                os.fsync(f.fileno())
            if expected_size is None and length is None and not expected_md5:
                raise DownloadError('file: нет метаданных для проверки полноты')
            if (expected_size is not None and size != expected_size) or (length is not None and size != length):
                raise DownloadError('file: оригинал получен не полностью')
            if expected_md5 and md5.hexdigest() != expected_md5:
                raise DownloadError('file: MD5 не совпадает')
            return size, sha.hexdigest()
    except requests.RequestException as e:
        # requests exception text contains signed URLs: record only its type.
        raise DownloadError(f'file: {type(e).__name__}',
                            is_timeout=is_request_timeout(e)) from None


def process_file(s, sk, uid, job, root, db_path, timeouts):
    dst = file_destination(root, job)
    tmp = dst + '.' + job['lease_token'] + '.part'
    try:
        with heartbeat(db_path, job) as lost:
            job['_phase'] = 'metadata'
            if '_worker_id' in job:
                log.info('worker=%s file_key=%s attempt=%s/%s phase=metadata start',
                         job['_worker_id'], job['_file_key'], job['_attempt'], job['_max_attempts'])
                print(f'[w{job["_worker_id"]}] file_key={job["_file_key"]} '
                      f'попытка {job["_attempt"]}/{job["_max_attempts"]}: метаданные', flush=True)
            info = original_info(s, sk, uid, job.get('download_path') or job['file_id'], timeouts)
            # Fresh metadata takes precedence over the old scan (the file may change).
            size = info['size']
            md5 = info['md5']
            queue.update_original_metadata(db_path, job, size, md5)
            existing = verified_existing(dst, size, md5)
            if existing:
                queue.complete(db_path, job, dst, existing[1], actual_size=existing[0], md5=md5)
                return existing[0]
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            # Only this ID's expired attempts, never other workers' active files.
            for orphan in Path(dst).parent.glob(Path(dst).name + '.*.part'):
                token = orphan.name[len(Path(dst).name) + 1:-5]
                if re.fullmatch(r'[a-f0-9]{32}', token) and token != job['lease_token']:
                    try:
                        orphan.unlink()
                    except OSError:
                        pass
            job['_phase'] = 'transfer'
            if '_worker_id' in job:
                log.info('worker=%s file_key=%s attempt=%s/%s phase=transfer start',
                         job['_worker_id'], job['_file_key'], job['_attempt'], job['_max_attempts'])
                print(f'[w{job["_worker_id"]}] file_key={job["_file_key"]} '
                      f'попытка {job["_attempt"]}/{job["_max_attempts"]}: передача', flush=True)
            actual_size, sha = download_file(s, info['url'], tmp, size, md5, timeouts, lost)
            queue.complete(db_path, job, dst, sha, tmp, actual_size=actual_size, md5=md5)
            return actual_size
    finally:
        try:
            os.remove(tmp)
        except FileNotFoundError:
            pass


def run(worker_id, run_id, root, db_path=DB_FILE, cookies_file=COOKIES_FILE,
        timeouts=None, timeout_retries=None):
    timeouts = timeouts or Timeouts.from_env()
    if timeout_retries is None:
        from runtime_settings import read_unlim_timeout_retries
        timeout_retries = read_unlim_timeout_retries()
    if not isinstance(timeout_retries, int) or isinstance(timeout_retries, bool) or not 0 <= timeout_retries <= 10:
        raise ValueError('UNLIM_TIMEOUT_RETRIES должен быть целым числом от 0 до 10')
    queue.ensure_schema(db_path)
    queue.reconcile_missing(db_path)
    cooldown = queue.cooldown_remaining(db_path)
    if cooldown:
        print(f'[w{worker_id}] Яндекс ограничил частоту; повторите запуск через {int(cooldown)+1} с')
        return 2
    total = 0
    current = None
    skips = SkipReporter(worker_id)
    with make_session(cookies_file) as s:
        sk, uid = get_sk_uid(s, timeouts)
        while True:
            current = queue.claim(db_path, worker_id, run_id)
            if current is None:
                break
            key = hashlib.sha256(current['file_id'].encode()).hexdigest()[:12]
            max_attempts = timeout_retries + 1
            try:
                for attempt in range(1, max_attempts + 1):
                    current.update(_worker_id=worker_id, _file_key=key,
                                   _attempt=attempt, _max_attempts=max_attempts,
                                   _phase='start')
                    start = time.monotonic()
                    log.info('worker=%s file_key=%s attempt=%s/%s start',
                             worker_id, key, attempt, max_attempts)
                    print(f'[w{worker_id}] file_key={key} попытка {attempt}/{max_attempts} начата', flush=True)
                    try:
                        size = process_file(s, sk, uid, current, root, db_path, timeouts)
                    except DownloadError as e:
                        elapsed = time.monotonic() - start
                        is_404 = bool(re.search(r': HTTP 404(?:\b|;)', e.code))
                        log.warning('worker=%s file_key=%s attempt=%s/%s phase=%s error=%s timeout=%s duration=%.1fs',
                                    worker_id, key, attempt, max_attempts, current['_phase'], e.code,
                                    e.is_timeout, elapsed)
                        if not is_404:
                            timeout_note = ' [таймаут]' if e.is_timeout else ''
                            print(f'[w{worker_id}] file_key={key} попытка {attempt}/{max_attempts} '
                                  f'фаза={current["_phase"]}: ошибка {e.code}{timeout_note} ({elapsed:.1f}с)', flush=True)
                        if e.fatal or not e.is_timeout or attempt == max_attempts:
                            raise
                        # Keep the same fenced claim across attempts and refresh its lease.
                        if not queue.renew(db_path, current):
                            raise RuntimeError('worker lease lost')
                        log.info('worker=%s file_key=%s timeout retry=%s/%s',
                                 worker_id, key, attempt + 1, max_attempts)
                        print(f'[w{worker_id}] file_key={key} таймаут; повтор '
                              f'{attempt + 1}/{max_attempts}', flush=True)
                    else:
                        elapsed = time.monotonic() - start
                        total += 1
                        log.info('worker=%s file_key=%s attempt=%s/%s success bytes=%s duration=%.1fs',
                                 worker_id, key, attempt, max_attempts, size, elapsed)
                        print(f'[w{worker_id}] file_key={key} попытка {attempt}/{max_attempts}: '
                              f'✓ file {total}: {size/1e6:.1f} MB ({elapsed:.1f}с)', flush=True)
                        break
            except DownloadError as e:
                queue.fail(db_path, current, e.code, delay=e.retry_after, rate_limited=bool(e.retry_after))
                if e.fatal:
                    skips.flush()
                    print(f'[w{worker_id}] остановка file_key={key}: {e.code}', flush=True)
                    return 2
                log.info('worker=%s file_key=%s deferred error=%s attempts=%s/%s',
                         worker_id, key, e.code, attempt, max_attempts)
                if re.search(r': HTTP 404(?:\b|;)', e.code):
                    skips.record(current['file_id'], e.code)
                else:
                    print(f'[w{worker_id}] file_key={key} отложен до следующего запуска: '
                          f'{e.code} (попыток {attempt}/{max_attempts})', flush=True)
            except OSError as e:
                # A full local disk is not an unavailable remote file: stop instead of failing the whole queue.
                queue.fail(db_path, current, f'local I/O: {type(e).__name__} errno={e.errno}')
                log.warning('worker=%s file_key=%s attempt=%s/%s phase=%s error=local I/O %s errno=%s duration=%.1fs',
                            worker_id, key, attempt, max_attempts, current['_phase'], type(e).__name__, e.errno,
                            time.monotonic() - start)
                skips.flush()
                print(f'[w{worker_id}] file_key={key} попытка {attempt}/{max_attempts} '
                      f'ошибка локального диска (errno={e.errno}); остановка', flush=True)
                return 2
            except BaseException:
                skips.flush()
                queue.release(db_path, current)
                raise
            finally:
                current = None
    skips.flush()
    stats = queue.counts(db_path)
    print(f'[w{worker_id}] завершено: {total}; недоступных в БД: {stats.get("failed",0)}', flush=True)
    return 1 if stats.get('failed', 0) or stats.get('pending', 0) else 0


def main(argv=None):
    load_dotenv(PROJECT / '.env')
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--id', type=int, required=True)
    ap.add_argument('--run-id', default=None, help='Общий ID прохода для параллельных воркеров')
    ap.add_argument('--batch', type=int, default=None, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    if args.id < 1:
        ap.error('--id должен быть положительным')
    if not Path(DB_FILE).exists():
        ap.error('Нет unlim_state.db — сначала запустите unlim_download.py --collect')
    if args.batch is not None:
        print('--batch больше не используется: скачивание всегда по одному оригиналу')
    run_id = args.run_id or uuid.uuid4().hex
    log_dir = PROJECT / 'logs'
    log_dir.mkdir(exist_ok=True)
    logging.basicConfig(filename=log_dir / 'unlim_worker.log', level=logging.INFO,
                        encoding='utf-8', format='%(asctime)s | %(levelname)s | %(message)s')
    # Translate normal process termination into cleanup of the owned job/.part.
    def terminate(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, terminate)
    root = Path(os.getenv('DOWNLOAD_DIR') or PROJECT / 'downloads').expanduser().resolve() / '_unlim'
    timeouts = Timeouts.from_env()
    print(f'[w{args.id}] одиночные оригиналы; connect={timeouts.connect}s, read={timeouts.read}s; run={run_id}', flush=True)
    return run(args.id, run_id, root, timeouts=timeouts)


if __name__ == '__main__':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    try:
        sys.exit(main())
    except DownloadError as e:
        print(f'Ошибка: {e.code}', flush=True)
        sys.exit(2 if e.fatal else 1)
    except (OSError, ValueError) as e:
        print(f'Ошибка запуска: {type(e).__name__}', flush=True)
        sys.exit(2)
    except KeyboardInterrupt:
        sys.exit(130)
