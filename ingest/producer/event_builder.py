"""
event_builder.py
Maps a raw timeline row into the canonical event envelope defined in
data-contracts/ingested-data.md §4 + §5.
"""
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

SCHEMA_VERSION = "1.0"
SOURCE_SYSTEM  = "olist_simulator"

_SOURCE_TABLE = {
    "olist.order_events":     "orders",
    "olist.payment_events":   "order_payments",
    "olist.delivery_events":  "orders",
    "olist.review_events":    "order_reviews",
    "olist.inventory_events": "order_items",
}


def build_envelope(row: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
    """
    Build a canonical Kafka message from a timeline row.

    Returns
    -------
    topic          : str   – target Kafka topic
    partition_key  : str   – order_id (guarantees ordering per contract §2)
    message        : dict  – full envelope ready to JSON-serialise
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    event_ts = row["event_ts"]
    event_ts_str = event_ts.isoformat() if hasattr(event_ts, "isoformat") else str(event_ts)

    topic    = row["topic"]
    order_id = str(row["order_id"]) if row.get("order_id") else str(uuid.uuid4())

    message: Dict[str, Any] = {
        # ── envelope (required by contract §4) ───────────────────────────────
        "event_id":         str(uuid.uuid4()),
        "event_type":       row["event_type"],
        "schema_version":   SCHEMA_VERSION,
        "event_ts":         event_ts_str,
        "ingestion_hint_ts": now_utc,
        "source_system":    SOURCE_SYSTEM,
        "source_table":     _SOURCE_TABLE.get(topic, "unknown"),
        "trace_id":         str(uuid.uuid4()),
        "order_id":         order_id,
        "customer_id":      str(row["customer_id"]) if row.get("customer_id") else None,
        "seller_id":        str(row["seller_id"])   if row.get("seller_id")   else None,
        # ── topic-specific payload (contract §5) ─────────────────────────────
        "payload":          row.get("payload", {}),
    }

    return topic, order_id, message
