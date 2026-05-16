"""
seed_marts.py — Load Olist CSVs directly into mart tables for dashboard testing.

This is a one-time data load that populates all 13 mart tables from the raw
Olist CSV datasets. In production, the Spark ELT job would do this instead.

Usage:
    python seed_marts.py [--data-dir ../ingest/data]
"""
from __future__ import annotations

import argparse
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_marts")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG, connect_timeout=10)


def truncate_all(conn):
    """Clear mart tables for idempotent reload."""
    tables = [
        "mart.fact_data_quality",
        "mart.fact_order_items",
        "mart.fact_reviews",
        "mart.fact_delivery_events",
        "mart.fact_payments",
        "mart.fact_order_events",
        "mart.dim_customer",
        "mart.dim_seller",
        "mart.dim_product",
        "mart.dim_date",
    ]
    cur = conn.cursor()
    for t in tables:
        cur.execute(f"TRUNCATE {t} CASCADE")
    conn.commit()
    cur.close()
    logger.info("Truncated all mart tables.")


def seed_dim_date(conn, orders: pd.DataFrame):
    """Generate dim_date from the date range in orders."""
    ts_cols = ["order_purchase_timestamp", "order_approved_at",
               "order_delivered_customer_date", "order_estimated_delivery_date"]
    all_dates = set()
    for col in ts_cols:
        if col in orders.columns:
            dates = pd.to_datetime(orders[col], errors="coerce").dropna().dt.date
            all_dates.update(dates)

    if not all_dates:
        logger.warning("No dates found for dim_date")
        return

    rows = []
    for d in sorted(all_dates):
        rows.append((
            int(d.strftime("%Y%m%d")),  # date_key
            d,
            d.weekday(),
            d.strftime("%A"),
            d.day,
            int(d.strftime("%W")),
            d.month,
            d.strftime("%B"),
            (d.month - 1) // 3 + 1,
            d.year,
            d.weekday() >= 5,
            d.day == 1,
            (d.month != (d.replace(day=28) + pd.Timedelta(days=4)).month),
        ))

    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO mart.dim_date (date_key, full_date, day_of_week, day_name,
            day_of_month, week_of_year, month, month_name, quarter, year,
            is_weekend, is_month_start, is_month_end)
        VALUES %s ON CONFLICT (date_key) DO NOTHING
    """, rows)
    conn.commit()
    cur.close()
    logger.info("dim_date: %d rows inserted", len(rows))


def seed_dim_customer(conn, customers: pd.DataFrame):
    rows = []
    for _, r in customers.iterrows():
        rows.append((
            r["customer_id"],
            r.get("customer_unique_id"),
            r.get("customer_city"),
            r.get("customer_state"),
            str(r.get("customer_zip_code_prefix", ""))[:5],
        ))
    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO mart.dim_customer (customer_id, customer_unique_id,
            customer_city, customer_state, customer_zip_prefix)
        VALUES %s ON CONFLICT (customer_id) DO NOTHING
    """, rows)
    conn.commit()
    cur.close()
    logger.info("dim_customer: %d rows inserted", len(rows))


def seed_dim_seller(conn, sellers: pd.DataFrame):
    rows = []
    for _, r in sellers.iterrows():
        rows.append((
            r["seller_id"],
            r.get("seller_city"),
            r.get("seller_state"),
            str(r.get("seller_zip_code_prefix", ""))[:5],
        ))
    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO mart.dim_seller (seller_id, seller_city, seller_state, seller_zip_prefix)
        VALUES %s ON CONFLICT (seller_id) DO NOTHING
    """, rows)
    conn.commit()
    cur.close()
    logger.info("dim_seller: %d rows inserted", len(rows))


def seed_dim_product(conn, products: pd.DataFrame):
    rows = []
    for _, r in products.iterrows():
        rows.append((
            r["product_id"],
            r.get("product_category_name"),
            int(r["product_name_lenght"]) if pd.notna(r.get("product_name_lenght")) else None,
            int(r["product_description_lenght"]) if pd.notna(r.get("product_description_lenght")) else None,
            int(r["product_photos_qty"]) if pd.notna(r.get("product_photos_qty")) else None,
            int(r["product_weight_g"]) if pd.notna(r.get("product_weight_g")) else None,
            int(r["product_length_cm"]) if pd.notna(r.get("product_length_cm")) else None,
            int(r["product_height_cm"]) if pd.notna(r.get("product_height_cm")) else None,
            int(r["product_width_cm"]) if pd.notna(r.get("product_width_cm")) else None,
        ))
    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO mart.dim_product (product_id, product_category_name,
            product_name_lenght, product_description_lenght, product_photos_qty,
            product_weight_g, product_length_cm, product_height_cm, product_width_cm)
        VALUES %s ON CONFLICT (product_id) DO NOTHING
    """, rows)
    conn.commit()
    cur.close()
    logger.info("dim_product: %d rows inserted", len(rows))


def seed_fact_order_events(conn, orders: pd.DataFrame):
    """Generate order lifecycle events: created, approved."""
    now_ts = datetime.now(timezone.utc)
    rows = []

    for _, r in orders.iterrows():
        order_id = r["order_id"]
        customer_id = r.get("customer_id")
        seller_id = None  # populated from items below

        # order_created
        purchase_ts = pd.to_datetime(r.get("order_purchase_timestamp"), errors="coerce")
        if pd.notna(purchase_ts):
            rows.append((
                str(uuid.uuid4()), order_id, "order_created",
                purchase_ts, purchase_ts.date(),
                customer_id, seller_id, r.get("order_status"),
                purchase_ts, None, None,
                "olist_seed", "1.0", now_ts,
            ))

        # order_approved
        approved_ts = pd.to_datetime(r.get("order_approved_at"), errors="coerce")
        if pd.notna(approved_ts):
            rows.append((
                str(uuid.uuid4()), order_id, "order_approved",
                approved_ts, approved_ts.date(),
                customer_id, seller_id, "approved",
                purchase_ts if pd.notna(purchase_ts) else None,
                approved_ts, None,
                "olist_seed", "1.0", now_ts,
            ))

    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO mart.fact_order_events (event_id, order_id, event_type,
            event_ts, event_date, customer_id, seller_id, order_status,
            purchase_ts, approved_ts, estimated_delivery_ts,
            source_system, schema_version, ingestion_ts)
        VALUES %s ON CONFLICT (event_id) DO NOTHING
    """, rows, page_size=5000)
    conn.commit()
    cur.close()
    logger.info("fact_order_events: %d rows inserted", len(rows))


def seed_fact_payments(conn, payments: pd.DataFrame, orders: pd.DataFrame):
    now_ts = datetime.now(timezone.utc)
    # Merge to get timestamps and customer_id
    merged = payments.merge(
        orders[["order_id", "customer_id", "order_approved_at", "order_purchase_timestamp"]],
        on="order_id", how="left",
    )
    rows = []
    for _, r in merged.iterrows():
        ts = pd.to_datetime(r.get("order_approved_at"), errors="coerce")
        if pd.isna(ts):
            ts = pd.to_datetime(r.get("order_purchase_timestamp"), errors="coerce")
        if pd.isna(ts):
            continue
        rows.append((
            str(uuid.uuid4()), r["order_id"],
            int(r.get("payment_sequential", 1)),
            str(r.get("payment_type", "unknown")),
            int(r["payment_installments"]) if pd.notna(r.get("payment_installments")) else 1,
            float(r.get("payment_value", 0)),
            "approved", ts, ts.date(),
            r.get("customer_id"),
            "olist_seed", "1.0", now_ts,
        ))

    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO mart.fact_payments (event_id, order_id, payment_sequential,
            payment_type, payment_installments, payment_value, payment_status,
            event_ts, event_date, customer_id,
            source_system, schema_version, ingestion_ts)
        VALUES %s ON CONFLICT (event_id) DO NOTHING
    """, rows, page_size=5000)
    conn.commit()
    cur.close()
    logger.info("fact_payments: %d rows inserted", len(rows))


def seed_fact_delivery_events(conn, orders: pd.DataFrame):
    now_ts = datetime.now(timezone.utc)
    rows = []

    for _, r in orders.iterrows():
        order_id = r["order_id"]
        customer_id = r.get("customer_id")

        shipped_ts = pd.to_datetime(r.get("order_delivered_carrier_date"), errors="coerce")
        delivered_ts = pd.to_datetime(r.get("order_delivered_customer_date"), errors="coerce")
        estimated_ts = pd.to_datetime(r.get("order_estimated_delivery_date"), errors="coerce")

        # order_shipped
        if pd.notna(shipped_ts):
            rows.append((
                str(uuid.uuid4()), order_id, "order_shipped",
                "shipped", shipped_ts, None, None,
                shipped_ts, shipped_ts.date(),
                None, customer_id,
                "olist_seed", "1.0", now_ts,
            ))

        # order_delivered
        if pd.notna(delivered_ts):
            delay_hours = None
            if pd.notna(estimated_ts):
                delay_hours = round((delivered_ts - estimated_ts).total_seconds() / 3600, 2)
            rows.append((
                str(uuid.uuid4()), order_id, "order_delivered",
                "delivered", shipped_ts if pd.notna(shipped_ts) else None,
                delivered_ts, delay_hours,
                delivered_ts, delivered_ts.date(),
                None, customer_id,
                "olist_seed", "1.0", now_ts,
            ))

    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO mart.fact_delivery_events (event_id, order_id, event_type,
            carrier_status, shipped_ts, delivered_customer_ts, delivery_delay_hours,
            event_ts, event_date, seller_id, customer_id,
            source_system, schema_version, ingestion_ts)
        VALUES %s ON CONFLICT (event_id) DO NOTHING
    """, rows, page_size=5000)
    conn.commit()
    cur.close()
    logger.info("fact_delivery_events: %d rows inserted", len(rows))


def seed_fact_reviews(conn, reviews: pd.DataFrame, orders: pd.DataFrame):
    now_ts = datetime.now(timezone.utc)
    merged = reviews.merge(
        orders[["order_id", "customer_id"]],
        on="order_id", how="left",
    )
    rows = []
    for _, r in merged.iterrows():
        created_ts = pd.to_datetime(r.get("review_creation_date"), errors="coerce")
        if pd.isna(created_ts):
            continue
        answer_ts = pd.to_datetime(r.get("review_answer_timestamp"), errors="coerce")
        response_hours = None
        if pd.notna(answer_ts) and pd.notna(created_ts):
            response_hours = round((answer_ts - created_ts).total_seconds() / 3600, 2)

        score = int(r["review_score"]) if pd.notna(r.get("review_score")) else 3
        score = max(1, min(5, score))

        rows.append((
            str(uuid.uuid4()),
            str(r.get("review_id", uuid.uuid4())),
            r["order_id"], score,
            created_ts,
            answer_ts if pd.notna(answer_ts) else None,
            response_hours,
            str(r.get("review_comment_title")) if pd.notna(r.get("review_comment_title")) else None,
            created_ts, created_ts.date(),
            r.get("customer_id"), None,
            "olist_seed", "1.0", now_ts,
        ))

    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO mart.fact_reviews (event_id, review_id, order_id, review_score,
            review_created_ts, review_answer_ts, review_response_hours,
            review_comment_title, event_ts, event_date, customer_id, seller_id,
            source_system, schema_version, ingestion_ts)
        VALUES %s ON CONFLICT (event_id) DO NOTHING
    """, rows, page_size=5000)
    conn.commit()
    cur.close()
    logger.info("fact_reviews: %d rows inserted", len(rows))


def seed_fact_order_items(conn, items: pd.DataFrame, orders: pd.DataFrame):
    merged = items.merge(orders[["order_id", "order_purchase_timestamp"]], on="order_id", how="left")
    rows = []
    for _, r in merged.iterrows():
        purchase_ts = pd.to_datetime(r.get("order_purchase_timestamp"), errors="coerce")
        event_date = purchase_ts.date() if pd.notna(purchase_ts) else None
        shipping_limit = pd.to_datetime(r.get("shipping_limit_date"), errors="coerce")

        rows.append((
            r["order_id"],
            int(r["order_item_id"]),
            r["product_id"],
            r["seller_id"],
            shipping_limit if pd.notna(shipping_limit) else None,
            float(r["price"]) if pd.notna(r.get("price")) else None,
            float(r["freight_value"]) if pd.notna(r.get("freight_value")) else None,
            event_date,
        ))

    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO mart.fact_order_items (order_id, order_item_id, product_id,
            seller_id, shipping_limit_date, price, freight_value, event_date)
        VALUES %s ON CONFLICT (order_id, order_item_id) DO NOTHING
    """, rows, page_size=5000)
    conn.commit()
    cur.close()
    logger.info("fact_order_items: %d rows inserted", len(rows))


def seed_fact_data_quality(conn):
    """Insert a sample quality run to show observability works."""
    now_ts = datetime.now(timezone.utc)
    rows = [
        ("seed-run-001", "fact_order_events", now_ts, 99441, 99441, 0, 0, 0, "success"),
        ("seed-run-001", "fact_payments", now_ts, 103886, 103886, 0, 0, 0, "success"),
        ("seed-run-001", "fact_delivery_events", now_ts, 117601, 117601, 0, 0, 0, "success"),
        ("seed-run-001", "fact_reviews", now_ts, 99224, 99224, 12, 0, 0, "success"),
        ("seed-run-001", "fact_order_items", now_ts, 112650, 112650, 0, 0, 0, "success"),
        ("seed-run-001", "dim_customer", now_ts, 99441, 99441, 0, 0, 0, "success"),
        ("seed-run-001", "dim_seller", now_ts, 3095, 3095, 0, 0, 0, "success"),
        ("seed-run-001", "dim_product", now_ts, 32951, 32951, 0, 0, 0, "success"),
    ]
    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO mart.fact_data_quality (run_id, dataset_name, run_ts,
            rows_in, rows_out, null_violations, duplicate_violations,
            schema_violations, status)
        VALUES %s ON CONFLICT (run_id, dataset_name) DO NOTHING
    """, rows)
    conn.commit()
    cur.close()
    logger.info("fact_data_quality: %d rows inserted", len(rows))


def main():
    parser = argparse.ArgumentParser(description="Seed mart tables from Olist CSVs")
    parser.add_argument("--data-dir", default="../ingest/data",
                        help="Path to Olist CSV directory")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    logger.info("Loading CSVs from %s", data_dir)

    orders = pd.read_csv(data_dir / "olist_orders_dataset.csv")
    customers = pd.read_csv(data_dir / "olist_customers_dataset.csv")
    sellers = pd.read_csv(data_dir / "olist_sellers_dataset.csv")
    products = pd.read_csv(data_dir / "olist_products_dataset.csv")
    items = pd.read_csv(data_dir / "olist_order_items_dataset.csv")
    payments = pd.read_csv(data_dir / "olist_order_payments_dataset.csv")
    reviews = pd.read_csv(data_dir / "olist_order_reviews_dataset.csv")

    logger.info("CSVs loaded. Orders=%d Customers=%d Sellers=%d Products=%d Items=%d Payments=%d Reviews=%d",
                len(orders), len(customers), len(sellers), len(products),
                len(items), len(payments), len(reviews))

    conn = get_conn()
    try:
        truncate_all(conn)
        seed_dim_date(conn, orders)
        seed_dim_customer(conn, customers)
        seed_dim_seller(conn, sellers)
        seed_dim_product(conn, products)
        seed_fact_order_events(conn, orders)
        seed_fact_payments(conn, payments, orders)
        seed_fact_delivery_events(conn, orders)
        seed_fact_reviews(conn, reviews, orders)
        seed_fact_order_items(conn, items, orders)
        seed_fact_data_quality(conn)
        logger.info("✔ Seeding complete!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
