"""Helpers for useful exception diagnostics without logging connection secrets."""

import re
import traceback
from typing import Tuple


_CONNECTION_URL = re.compile(
    r"(?i)\b(?:amqps?|rediss?|https?)://[^\s'\"<>]+"
)
_USERINFO = re.compile(r"(?i)\b[^\s/@:]+:[^\s/@]+@")
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)([\"']?\b(password|passwd|token|secret|credential|authorization)"
    r"[\"']?\s*[:=]\s*[\"']?)([^\"'\s,;}]+)"
)
_BEARER_TOKEN = re.compile(r"(?i)(\bauthorization\s*[:=]\s*bearer\s+)\S+")


def _redact(text: str) -> str:
    text = _CONNECTION_URL.sub("<redacted-connection-url>", text)
    text = _USERINFO.sub("<redacted-userinfo>@", text)
    text = _BEARER_TOKEN.sub(r"\1<redacted>", text)
    return _SECRET_ASSIGNMENT.sub(r"\1<redacted>", text)


def safe_exception_details(exc: BaseException) -> Tuple[str, str]:
    """Return exception details and traceback with common secret forms removed."""
    details = _redact(str(exc))[:2000]
    formatted = "".join(traceback.TracebackException.from_exception(
        exc, capture_locals=False
    ).format())
    return details, _redact(formatted)[-12000:]
