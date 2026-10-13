"""Small, safe stdout diagnostics for the web server and its child processes."""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from typing import Any


_WRITE_LOCK = threading.Lock()
_URL = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_COOKIE_HEADER = re.compile(r"(?i)\b(?:set-cookie|cookies?)\s*[:=]\s*[^\r\n]+")
_AUTH_HEADER = re.compile(r"(?i)\bauthorization\s*[:=]\s*[^\r\n]+")
_CREDENTIAL = re.compile(
    r"(?i)\b(token|access_token|refresh_token|oauth_token|cookie|set-cookie|"
    r"authorization|password|passwd|secret|api[_-]?key|sk|uid|"
    r"session[_-]?id\d*|yandexuid)"
    r"([\"']?\s*[:=]\s*)(?:Bearer\s+)?(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"
)
_BEARER = re.compile(r"(?i)\bBearer\s+[^\s,;]+")
_OAUTH_TOKEN = re.compile(r"(?i)\b(?:y0_|AQVN)[A-Za-z0-9._~+/-]+")
_SK_TOKEN = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")
_ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_SENSITIVE_KEY = re.compile(
    r"(?i)(?:^|[_-])(token|cookie|authorization|password|passwd|"
    r"secret|api[_-]?key|sk|uid|session[_-]?id\d*|yandexuid)(?:$|[_-])"
)


def redact(value: Any) -> str:
    """Remove common credentials and URLs from untrusted diagnostic text."""
    result = _ANSI.sub("", str(value))
    result = _URL.sub("[URL_REDACTED]", result)
    result = _COOKIE_HEADER.sub("cookie=[REDACTED]", result)
    result = _AUTH_HEADER.sub("authorization=[REDACTED]", result)
    result = _CREDENTIAL.sub(lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]", result)
    result = _BEARER.sub("Bearer [REDACTED]", result)
    result = _OAUTH_TOKEN.sub("[REDACTED]", result)
    return _SK_TOKEN.sub("[REDACTED]", result)


def _safe_value(key: str, value: Any) -> str:
    if _SENSITIVE_KEY.search(key):
        return "[REDACTED]"
    if isinstance(value, (dict, list, tuple)):
        value = json.dumps(value, ensure_ascii=False, default=str)
    return redact(value)


def emit(event: str, **fields: Any) -> None:
    """Write one timestamped event to the current stdout, then flush it."""
    safe_event = re.sub(r"[^\w.-]", "_", str(event)) or "event"
    stamp = datetime.now().astimezone().isoformat(timespec="seconds")
    parts = [stamp, safe_event]
    for key, value in fields.items():
        safe_key = re.sub(r"[^\w.-]", "_", str(key)) or "field"
        parts.append(f"{safe_key}={json.dumps(_safe_value(safe_key, value), ensure_ascii=False)}")
    with _WRITE_LOCK:
        print(" ".join(parts), file=sys.stdout, flush=True)


def relay(slot: str, line: str) -> None:
    """Relay one child-process line without exposing common credentials."""
    # One logical line per event, even when the process writes control characters.
    clean = redact(line).replace("\r", " ").replace("\n", " ").strip()
    if clean:
        emit("child_output", slot=slot, line=clean[:2000])


class DiagnosticsMonitor:
    """Emit a heartbeat and per-task status at a fixed interval."""

    def __init__(
        self,
        snapshot: Callable[[], Iterable[Mapping[str, Any]]],
        interval: float = 30.0,
    ) -> None:
        if interval <= 0:
            raise ValueError("interval must be positive")
        self.snapshot = snapshot
        self.interval = interval
        self.started_at = time.monotonic()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def report(self) -> None:
        """Emit one deterministic sample; useful for manual checks and tests."""
        try:
            tasks = list(self.snapshot())
        except Exception as exc:
            emit("heartbeat_error", error=type(exc).__name__)
            return
        active = sum(bool(task.get("running")) for task in tasks)
        load = {}
        if hasattr(os, "getloadavg"):
            try:
                load["load_1m"] = round(os.getloadavg()[0], 2)
            except OSError:
                pass
        emit(
            "server_heartbeat",
            uptime_s=round(time.monotonic() - self.started_at, 1),
            active=active,
            threads=threading.active_count(),
            pid=os.getpid(),
            **load,
        )
        for task in tasks:
            if not task.get("running"):
                continue
            emit(
                "task_status",
                slot=task.get("slot", "?"),
                pid=task.get("pid", "?"),
                running=True,
                elapsed_s=round(float(task.get("elapsed", 0)), 1),
                idle_s=round(float(task.get("idle", 0)), 1),
            )

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self.report()

    def start(self) -> None:
        """Start once; repeated calls do not create another monitor thread."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, name="server-diagnostics", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        """Stop promptly, allowing at most two seconds for a running report."""
        with self._lock:
            thread = self._thread
            self._stop.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)
