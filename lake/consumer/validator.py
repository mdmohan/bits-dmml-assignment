"""
validator.py
Validate raw Kafka messages against the Olist event envelope (contract v1).

Returns (is_valid, parsed_or_raw, error_reason).
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, Tuple

logger = logging.getLogger(__name__)

# Required envelope fields per data-contracts/ingested-data.md §4
_REQUIRED_FIELDS = (
    "event_id",
    "event_type",
    "schema_version",
    "event_ts",
    "source_system",
    "order_id",
    "payload",
)

_ISO_TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$"
)


def _is_iso8601(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    if _ISO_TS_RE.match(value):
        return True
    # Fallback: try fromisoformat (handles "2026-05-12T10:20:30+00:00")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def validate(raw_bytes: bytes) -> Tuple[bool, Dict[str, Any] | str, str | None]:
    """
    Returns:
        (True,  parsed_dict, None)        on success
        (False, raw_string,  reason)      on failure
    """
    if raw_bytes is None:
        return False, "", "null_message"

    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        return False, repr(raw_bytes), f"decode_error: {exc}"

    try:
        msg = json.loads(text)
    except json.JSONDecodeError as exc:
        return False, text, f"invalid_json: {exc.msg} at pos {exc.pos}"

    if not isinstance(msg, dict):
        return False, text, "envelope_not_object"

    missing = [f for f in _REQUIRED_FIELDS if f not in msg]
    if missing:
        return False, text, f"missing_required_fields: {','.join(missing)}"

    if not isinstance(msg.get("payload"), dict):
        return False, text, "payload_not_object"

    if not _is_iso8601(msg.get("event_ts")):
        return False, text, "event_ts_not_iso8601"

    return True, msg, None
