"""Shared effective network timeouts for UI and worker processes."""

import os


def read_unlim_timeout_retries() -> int:
    """Extra attempts after a per-file timeout; keep fast skipping by default."""
    try:
        value = int(os.getenv("UNLIM_TIMEOUT_RETRIES", "0"))
    except (TypeError, ValueError):
        return 0
    return value if 0 <= value <= 10 else 0


def read_timeouts() -> tuple[float, float]:
    values = []
    for key, default in (("DOWNLOAD_CONNECT_TIMEOUT", 5.0),
                         ("DOWNLOAD_READ_TIMEOUT", 15.0)):
        try:
            value = float(os.getenv(key, str(default)))
        except (TypeError, ValueError):
            value = default
        values.append(value if 0.1 <= value <= 3600 else default)
    return tuple(values)
