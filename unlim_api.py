"""Small Photoslice web API adapter with bounded waits and secret-free errors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import re
import time
from urllib.parse import urlsplit
import uuid

import requests
from urllib3.exceptions import ConnectTimeoutError, ReadTimeoutError

BASE = "https://disk.yandex.ru"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class Timeouts:
    connect: float = 5
    read: float = 15

    @classmethod
    def from_env(cls):
        from runtime_settings import read_timeouts

        return cls(*read_timeouts())

    @property
    def pair(self):
        return self.connect, self.read


class DownloadError(Exception):
    """Message is safe to persist/display: never includes response body or URL."""

    def __init__(self, code, *, fatal=False, retry_after=0, is_timeout=False):
        super().__init__(code)
        self.code = code
        self.fatal = fatal
        self.retry_after = retry_after
        self.is_timeout = is_timeout


def is_request_timeout(error):
    """Recognize actual timeout classes, including urllib3 errors wrapped by requests."""
    seen = set()
    pending = [error]
    while pending:
        item = pending.pop()
        if id(item) in seen:
            continue
        seen.add(id(item))
        if isinstance(
            item, (requests.exceptions.Timeout, ReadTimeoutError, ConnectTimeoutError)
        ):
            return True
        if isinstance(item, BaseException):
            pending.extend(arg for arg in item.args if isinstance(arg, BaseException))
            pending.extend(
                x
                for x in (
                    item.__cause__,
                    item.__context__,
                    getattr(item, "reason", None),
                )
                if isinstance(x, BaseException)
            )
    return False


def retry_after_seconds(value):
    try:
        return max(0, float(value))
    except (ValueError, TypeError):
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0, (dt - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return 60


def check_http(response, stage, auth=False):
    code = response.status_code
    if code == 429:
        raise DownloadError(
            f"{stage}: HTTP 429 — ограничение частоты",
            fatal=True,
            retry_after=retry_after_seconds(response.headers.get("Retry-After")),
        )
    if auth and (code in (401, 403) or 300 <= code < 400):
        raise DownloadError(
            f"{stage}: HTTP {code} — проверьте cookies/сессию", fatal=True
        )
    if code != 200:
        raise DownloadError(f"{stage}: HTTP {code}")


def checked_json(response, stage):
    try:
        data = response.json()
    except (ValueError, requests.exceptions.JSONDecodeError):
        raise DownloadError(
            f"{stage}: ответ не JSON (возможна авторизация/CAPTCHA)", fatal=True
        ) from None
    if not isinstance(data, dict):
        raise DownloadError(f"{stage}: изменилась структура JSON", fatal=True)
    if data.get("type") == "captcha":
        raise DownloadError(
            f"{stage}: CAPTCHA — требуется вход через браузер", fatal=True
        )
    err = data.get("error")
    if err:
        raw = err.get("code", "API_ERROR") if isinstance(err, dict) else err
        # Only emit identifiers, never arbitrary server messages/URLs.
        code = str(raw)
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", code):
            code = "API_ERROR"
        upper = code.upper()
        fatal = any(
            t in upper
            for t in ("AUTH", "CKEY", "CSRF", "CAPTCHA", "SESSION", "SK_EXPIRED")
        )
        raise DownloadError(f"{stage}: API {code}", fatal=fatal)
    return data


def make_session(cookies_file):
    with open(cookies_file, encoding="utf-8") as f:
        cookies = json.load(f)
    if not isinstance(cookies, list):
        raise DownloadError(
            "cookies.json должен содержать массив Cookie-Editor", fatal=True
        )
    s = requests.Session()
    s.max_redirects = 3
    s.headers.update(
        {
            "User-Agent": UA,
            "Accept": "*/*",
            "Accept-Language": "ru,en;q=0.9",
            "Origin": BASE,
            "Referer": BASE + "/client/photo",
            "X-Requested-With": "XMLHttpRequest",
        }
    )
    for c in cookies:
        if not isinstance(c, dict) or not c.get("name") or c.get("value") is None:
            continue
        domain = c.get("domain", ".yandex.ru")
        if domain.lstrip(".") != "yandex.ru" and not domain.lstrip(".").endswith(
            ".yandex.ru"
        ):
            continue
        s.cookies.set(
            c["name"],
            c["value"],
            domain=domain,
            path=c.get("path", "/"),
            secure=bool(c.get("secure", True)),
        )
    return s


def get_sk_uid(s, timeouts):
    try:
        with s.get(BASE + "/client/disk", timeout=timeouts.pair) as r:
            check_http(r, "session", auth=True)
            if urlsplit(r.url).hostname != "disk.yandex.ru":
                raise DownloadError(
                    "Сессия истекла — обновите cookies.json", fatal=True
                )
            sk = re.search(r'"sk"\s*:\s*"([^"]+)"', r.text)
            uid = re.search(r'"uid"\s*:\s*"?(\d+)"?', r.text)
            if not sk or not uid:
                raise DownloadError(
                    "В HTML нет sk/uid — проверьте вход/CAPTCHA или формат клиента",
                    fatal=True,
                )
            return sk.group(1), uid.group(1)
    except requests.RequestException as e:
        raise DownloadError(f"session: {type(e).__name__}", fatal=True) from None


def api_call(s, sk, uid, method, params=None, timeouts=None):
    timeouts = timeouts or Timeouts.from_env()
    body = {
        "sk": sk,
        "connection_id": f"{uid}{int(time.time() * 1000)}{uuid.uuid4().hex[:8]}",
        "apiMethod": method,
    }
    if params is not None:
        body["requestParams"] = params
    try:
        with s.post(
            BASE + "/models-v2",
            params={"m": method},
            json=body,
            timeout=timeouts.pair,
            allow_redirects=False,
        ) as r:
            if r.status_code == 404:
                # Keep the machine-readable reason without logging response bodies,
                # resource paths or signed URLs. HTTP 404 remains retryable next run.
                code = None
                try:
                    payload = r.json()
                    error = payload.get("error") if isinstance(payload, dict) else None
                    raw = error.get("code") if isinstance(error, dict) else None
                    if raw is not None and re.fullmatch(
                        r"[A-Za-z0-9_-]{1,64}", str(raw)
                    ):
                        code = str(raw)
                except ValueError:
                    pass
                detail = f"; API {code}" if code else ""
                raise DownloadError(f"{method}: HTTP 404{detail}")
            if r.status_code == 403:
                # A per-resource refusal is not necessarily an expired session.
                # CKEY/CAPTCHA/auth error codes still stop the run via checked_json.
                checked_json(r, method)
                check_http(r, method, auth=False)
            else:
                check_http(r, method, auth=True)
            return checked_json(r, method)
    except requests.RequestException as e:
        raise DownloadError(
            f"{method}: {type(e).__name__}", is_timeout=is_request_timeout(e)
        ) from None


def validate_download_url(value):
    if not isinstance(value, str) or not value:
        raise DownloadError("download: в ответе нет ссылки на оригинал", fatal=True)
    if value.startswith("//"):
        value = "https:" + value
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        raise DownloadError("download: недопустимая ссылка на оригинал", fatal=True)
    return value


def resource_path(file_id):
    """Convert Photoslice's photounlim:/x to mpfs's /photounlim/x."""
    if not isinstance(file_id, str) or not file_id:
        raise DownloadError("info: отсутствует путь ресурса")
    match = re.fullmatch(r"(?:\d+:)?(photounlim|disk):(/.*)", file_id, flags=re.DOTALL)
    if match:
        return "/" + match.group(1) + match.group(2)
    if file_id.startswith(("/photounlim/", "/disk/")):
        return file_id
    raise DownloadError("info: неподдерживаемый формат file_id (нужен путь ресурса)")


def original_info(s, sk, uid, file_id, timeouts):
    """mpfs/info -> meta.sizes[name=ORIGINAL].url; no previews or ZIP fallback.

    Contract reference: delorum/yaDiskUnlimDownloader, YandexDiskApi.scala.
    This internal API can change; fail explicitly instead of storing a preview.
    """
    data = api_call(s, sk, uid, "mpfs/info", {"path": resource_path(file_id)}, timeouts)
    meta = data.get("meta")
    if not isinstance(meta, dict):
        raise DownloadError("info: в ответе нет meta", fatal=True)
    sizes = meta.get("sizes") or []
    if not isinstance(sizes, list):
        raise DownloadError("info: неизвестный формат meta.sizes", fatal=True)
    original = next(
        (
            entry
            for entry in sizes
            if isinstance(entry, dict) and entry.get("name") == "ORIGINAL"
        ),
        None,
    )
    if not original:
        raise DownloadError("info: ссылка ORIGINAL недоступна")
    url = validate_download_url(original.get("url"))
    raw_size = meta.get("size", data.get("size"))
    try:
        size = int(raw_size) if raw_size is not None else None
        if size is not None and size < 0:
            raise ValueError()
    except (ValueError, TypeError):
        raise DownloadError("info: некорректный размер оригинала", fatal=True) from None
    raw_md5 = meta.get("md5") or data.get("md5")
    md5 = (
        raw_md5.lower()
        if isinstance(raw_md5, str) and re.fullmatch(r"[0-9a-fA-F]{32}", raw_md5)
        else None
    )
    return {"url": url, "size": size, "md5": md5}
