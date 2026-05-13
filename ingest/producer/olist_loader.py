"""
olist_loader.py
Loads Olist CSVs and builds a flat, chronologically sorted event timeline
for replay by the simulator.
"""
import logging
from pathlib import Path
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def load_all(data_dir: str) -> Dict[str, pd.DataFrame]:
    """Load all required Olist CSVs into a dict of DataFrames."""
    p = Path(data_dir)
    files = {
        "orders":   "olist_orders_dataset.csv",
        "items":    "olist_order_items_dataset.csv",
        "payments": "olist_order_payments_dataset.csv",
        "customers":"olist_customers_dataset.csv",
        "products": "olist_products_dataset.csv",
        "sellers":  "olist_sellers_dataset.csv",
        "reviews":  "olist_order_reviews_dataset.csv",
    }
    dfs = {}
    for key, fname in files.items():
        fpath = p / fname
        logger.info("Loading %s …", fpath)
        dfs[key] = pd.read_csv(fpath)
    return dfs


def build_event_timeline(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merge all Olist tables into a flat list of events sorted by event_ts.

    Each row has:
        event_ts       – datetime of the event
        event_type     – e.g. 'order_created'
        topic          – Kafka topic name
        order_id       – str
        customer_id    – str or None
        seller_id      – str or None
        payload        – dict (topic-specific fields per contract §5)
    """
    orders   = dfs["orders"].copy()
    items    = dfs["items"].copy()
    payments = dfs["payments"].copy()
    reviews  = dfs["reviews"].copy()

    # ── Parse timestamps ──────────────────────────────────────────────────────
    ts_cols = [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in ts_cols:
        if col in orders.columns:
            orders[col] = pd.to_datetime(orders[col], errors="coerce")

    # ── Per-order aggregates from items table ─────────────────────────────────
    item_agg = (
        items.groupby("order_id")
        .agg(item_count=("order_item_id", "count"), order_value=("price", "sum"))
        .reset_index()
    )
    primary_seller = (
        items.groupby("order_id")["seller_id"].first().reset_index()
    )
    orders = orders.merge(item_agg,      on="order_id", how="left")
    orders = orders.merge(primary_seller, on="order_id", how="left")

    events = []

    # ── order_created ─────────────────────────────────────────────────────────
    for _, row in orders[orders["order_purchase_timestamp"].notna()].iterrows():
        events.append({
            "event_ts":    row["order_purchase_timestamp"],
            "event_type":  "order_created",
            "topic":       "olist.order_events",
            "order_id":    row["order_id"],
            "customer_id": row.get("customer_id"),
            "seller_id":   row.get("seller_id"),
            "payload": {
                "order_status":           row.get("order_status", "created"),
                "purchase_ts":            str(row["order_purchase_timestamp"]),
                "approved_ts":            str(row["order_approved_at"]) if pd.notna(row.get("order_approved_at")) else None,
                "estimated_delivery_ts":  str(row["order_estimated_delivery_date"]) if pd.notna(row.get("order_estimated_delivery_date")) else None,
                "order_value":            float(row["order_value"]) if pd.notna(row.get("order_value")) else 0.0,
                "item_count":             int(row["item_count"])    if pd.notna(row.get("item_count"))    else 0,
            },
        })

    # ── order_approved ────────────────────────────────────────────────────────
    for _, row in orders[orders["order_approved_at"].notna()].iterrows():
        events.append({
            "event_ts":    row["order_approved_at"],
            "event_type":  "order_approved",
            "topic":       "olist.order_events",
            "order_id":    row["order_id"],
            "customer_id": row.get("customer_id"),
            "seller_id":   row.get("seller_id"),
            "payload": {
                "order_status": "approved",
                "purchase_ts":  str(row["order_purchase_timestamp"]) if pd.notna(row.get("order_purchase_timestamp")) else None,
                "approved_ts":  str(row["order_approved_at"]),
                "order_value":  float(row["order_value"]) if pd.notna(row.get("order_value")) else 0.0,
                "item_count":   int(row["item_count"])    if pd.notna(row.get("item_count"))    else 0,
            },
        })

    # ── payment_approved ──────────────────────────────────────────────────────
    pay = payments.merge(
        orders[["order_id", "order_approved_at", "order_purchase_timestamp", "customer_id", "seller_id"]],
        on="order_id", how="left",
    )
    for _, row in pay.iterrows():
        ts = row["order_approved_at"] if pd.notna(row.get("order_approved_at")) else row.get("order_purchase_timestamp")
        if pd.isna(ts):
            continue
        events.append({
            "event_ts":    ts,
            "event_type":  "payment_approved",
            "topic":       "olist.payment_events",
            "order_id":    row["order_id"],
            "customer_id": row.get("customer_id"),
            "seller_id":   row.get("seller_id"),
            "payload": {
                "payment_sequential":    int(row.get("payment_sequential", 1)),
                "payment_type":          str(row.get("payment_type", "unknown")),
                "payment_installments":  int(row.get("payment_installments", 1)),
                "payment_value":         float(row.get("payment_value", 0.0)),
                "payment_status":        "approved",
            },
        })

    # ── order_shipped ─────────────────────────────────────────────────────────
    for _, row in orders[orders["order_delivered_carrier_date"].notna()].iterrows():
        events.append({
            "event_ts":    row["order_delivered_carrier_date"],
            "event_type":  "order_shipped",
            "topic":       "olist.delivery_events",
            "order_id":    row["order_id"],
            "customer_id": row.get("customer_id"),
            "seller_id":   row.get("seller_id"),
            "payload": {
                "carrier_status":         "shipped",
                "shipped_ts":             str(row["order_delivered_carrier_date"]),
                "delivered_customer_ts":  str(row["order_delivered_customer_date"]) if pd.notna(row.get("order_delivered_customer_date")) else None,
                "delivery_delay_hours":   None,
            },
        })

    # ── order_delivered ───────────────────────────────────────────────────────
    for _, row in orders[orders["order_delivered_customer_date"].notna()].iterrows():
        delay_hours = None
        if pd.notna(row.get("order_estimated_delivery_date")):
            delta = row["order_delivered_customer_date"] - row["order_estimated_delivery_date"]
            delay_hours = round(delta.total_seconds() / 3600, 2)
        events.append({
            "event_ts":    row["order_delivered_customer_date"],
            "event_type":  "order_delivered",
            "topic":       "olist.delivery_events",
            "order_id":    row["order_id"],
            "customer_id": row.get("customer_id"),
            "seller_id":   row.get("seller_id"),
            "payload": {
                "carrier_status":         "delivered",
                "shipped_ts":             str(row["order_delivered_carrier_date"]) if pd.notna(row.get("order_delivered_carrier_date")) else None,
                "delivered_customer_ts":  str(row["order_delivered_customer_date"]),
                "delivery_delay_hours":   delay_hours,
            },
        })

    # ── review_created ────────────────────────────────────────────────────────
    reviews["review_creation_date"]   = pd.to_datetime(reviews["review_creation_date"],   errors="coerce")
    reviews["review_answer_timestamp"] = pd.to_datetime(reviews.get("review_answer_timestamp", pd.NaT), errors="coerce")
    rev = reviews.merge(orders[["order_id", "customer_id", "seller_id"]], on="order_id", how="left")
    for _, row in rev[rev["review_creation_date"].notna()].iterrows():
        events.append({
            "event_ts":    row["review_creation_date"],
            "event_type":  "review_created",
            "topic":       "olist.review_events",
            "order_id":    row["order_id"],
            "customer_id": row.get("customer_id"),
            "seller_id":   row.get("seller_id"),
            "payload": {
                "review_id":            str(row.get("review_id", "")),
                "review_score":         int(row.get("review_score", 0)),
                "review_created_ts":    str(row["review_creation_date"]),
                "review_answer_ts":     str(row["review_answer_timestamp"]) if pd.notna(row.get("review_answer_timestamp")) else None,
                "review_comment_title": str(row["review_comment_title"])    if pd.notna(row.get("review_comment_title"))    else None,
            },
        })

    df = pd.DataFrame(events).sort_values("event_ts").reset_index(drop=True)
    logger.info(
        "Event timeline built: %d events across topics: %s",
        len(df),
        df["topic"].value_counts().to_dict(),
    )
    return df
