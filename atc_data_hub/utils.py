from __future__ import annotations

import base64
import math
import os
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

OLE_AUTOMATION_EPOCH = datetime(1899, 12, 30)

_ATOMIC_RETRIES = 3
_ATOMIC_RETRY_DELAY = 0.2  # seconds


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_parent(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    """Write *content* to *path* atomically using a unique sibling temp file.

    Using a unique temp name (via tempfile) instead of a fixed ``.tmp`` suffix
    prevents ``PermissionError`` collisions when another process holds an open
    handle on the old temp file (common on Windows with antivirus or the legacy
    Protector service accessing the same output directory).

    Retries up to ``_ATOMIC_RETRIES`` times on ``PermissionError`` before
    re-raising, so a brief file-lock from an external reader does not crash the
    main loop.
    """
    ensure_parent(path)
    last_exc: Exception | None = None
    for attempt in range(_ATOMIC_RETRIES):
        fd, tmp_str = tempfile.mkstemp(
            dir=path.parent, prefix=path.stem + "_", suffix=path.suffix + ".tmp"
        )
        tmp = Path(tmp_str)
        try:
            os.write(fd, content.encode(encoding))
            os.close(fd)
            fd = -1
            tmp.replace(path)
            return
        except PermissionError as exc:
            last_exc = exc
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
                fd = -1
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            if attempt < _ATOMIC_RETRIES - 1:
                time.sleep(_ATOMIC_RETRY_DELAY)
        except Exception:
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise
    raise IOError(f"atomic_write_text failed after {_ATOMIC_RETRIES} attempts: {path}") from last_exc


def atomic_write_bytes(path: Path, content: bytes) -> None:
    """Write *content* to *path* atomically using a unique sibling temp file."""
    ensure_parent(path)
    last_exc: Exception | None = None
    for attempt in range(_ATOMIC_RETRIES):
        fd, tmp_str = tempfile.mkstemp(
            dir=path.parent, prefix=path.stem + "_", suffix=path.suffix + ".tmp"
        )
        tmp = Path(tmp_str)
        try:
            os.write(fd, content)
            os.close(fd)
            fd = -1
            tmp.replace(path)
            return
        except PermissionError as exc:
            last_exc = exc
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
                fd = -1
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            if attempt < _ATOMIC_RETRIES - 1:
                time.sleep(_ATOMIC_RETRY_DELAY)
        except Exception:
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise
    raise IOError(f"atomic_write_bytes failed after {_ATOMIC_RETRIES} attempts: {path}") from last_exc


def datetime_to_oadate(value: datetime) -> float:
    delta = value - OLE_AUTOMATION_EPOCH
    return delta.days + (delta.seconds + delta.microseconds / 1_000_000) / 86400


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def format_datetime(value: datetime | None) -> str | None:
    return value.isoformat(timespec="seconds") if value else None


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def format_date(value: date | None) -> str | None:
    return value.isoformat() if value else None


def encode_bytes(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def decode_bytes(value: str) -> bytes:
    return base64.b64decode(value.encode("ascii"))


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius * c


def recent_within(value: datetime | None, window_end: datetime, hours: int = 1) -> bool:
    if value is None:
        return False
    window_start = window_end - timedelta(hours=hours)
    return window_start <= value <= window_end


def percentage_over_capacity(count: int, capacity: int) -> str:
    if capacity <= 0 or count <= capacity:
        return ""
    percent = round((count - capacity) * 100 / capacity)
    return f"{percent}%"


def safe_json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return format_datetime(value)
    if isinstance(value, date):
        return format_date(value)
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")
