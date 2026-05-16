"""
queries.py — SQL queries for BI dashboards, analytics, and observability.
Organized by dashboard section.
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: KPI SUMMARY (Top-level metrics)
# ═══════════════════════════════════════════════════════════════════════════════

KPI_TOTAL_ORDERS = """
SELECT count(DISTINCT order_id) AS total_orders
FROM mart.fact_order_events
WHERE event_type = 'order_created';
"""

KPI_TOTAL_REVENUE = """
SELECT COALESCE(sum(payment_value), 0) AS total_revenue
FROM mart.fact_payments
WHERE payment_status = 'approved';
"""

KPI_AVG_ORDER_VALUE = """
SELECT COALESCE(avg(order_total), 0) AS avg_order_value
FROM (
    SELECT order_id, sum(payment_value) AS order_total
    FROM mart.fact_payments
    WHERE payment_status = 'approved'
    GROUP BY order_id
) sub;
"""

KPI_TOTAL_CUSTOMERS = """
SELECT count(*) AS total_customers FROM mart.dim_customer;
"""

KPI_AVG_REVIEW_SCORE = """
SELECT COALESCE(avg(review_score), 0) AS avg_review_score
FROM mart.fact_reviews;
"""

KPI_DELIVERY_ON_TIME_PCT = """
SELECT
    COALESCE(
        100.0 * sum(CASE WHEN delivery_delay_hours <= 0 THEN 1 ELSE 0 END)
        / NULLIF(count(*), 0),
        0
    ) AS on_time_pct
FROM mart.fact_delivery_events
WHERE event_type = 'order_delivered'
  AND delivery_delay_hours IS NOT NULL;
"""

KPI_TOTAL_SELLERS = """
SELECT count(*) AS total_sellers FROM mart.dim_seller;
"""

KPI_AVG_DELIVERY_DELAY = """
SELECT COALESCE(avg(delivery_delay_hours), 0) AS avg_delay_hours
FROM mart.fact_delivery_events
WHERE event_type = 'order_delivered'
  AND delivery_delay_hours IS NOT NULL;
"""

KPI_TOTAL_PRODUCTS_SOLD = """
SELECT count(*) AS products_sold FROM mart.fact_order_items;
"""

KPI_REVENUE_MOM = """
WITH monthly AS (
    SELECT
        date_trunc('month', event_date)::date AS month,
        sum(payment_value) AS revenue
    FROM mart.fact_payments
    WHERE payment_status = 'approved'
    GROUP BY 1
    ORDER BY 1 DESC
    LIMIT 2
)
SELECT
    COALESCE(
        100.0 * (curr.revenue - prev.revenue) / NULLIF(prev.revenue, 0),
        0
    ) AS mom_growth_pct,
    curr.revenue AS current_month_revenue,
    prev.revenue AS previous_month_revenue
FROM (SELECT revenue, month FROM monthly LIMIT 1) curr
CROSS JOIN (SELECT revenue, month FROM monthly OFFSET 1 LIMIT 1) prev;
"""

TOP5_CATEGORIES_OVERVIEW = """
SELECT
    COALESCE(p.product_category_name, 'Unknown') AS category,
    COALESCE(sum(i.price), 0) AS revenue
FROM mart.fact_order_items i
JOIN mart.dim_product p ON p.product_id = i.product_id
GROUP BY 1
ORDER BY revenue DESC
LIMIT 5;
"""

TOP5_STATES_OVERVIEW = """
SELECT
    c.customer_state AS state,
    count(DISTINCT e.order_id) AS orders
FROM mart.fact_order_events e
JOIN mart.dim_customer c ON c.customer_id = e.customer_id
WHERE e.event_type = 'order_created'
GROUP BY 1
ORDER BY orders DESC
LIMIT 5;
"""

BOTTOM5_SELLERS_ONTIME = """
SELECT
    s.seller_id,
    s.seller_state,
    count(*) AS deliveries,
    100.0 * sum(CASE WHEN d.delivery_delay_hours <= 0 THEN 1 ELSE 0 END)
        / NULLIF(count(*), 0) AS on_time_pct
FROM mart.fact_delivery_events d
JOIN mart.dim_seller s ON s.seller_id = d.seller_id
WHERE d.event_type = 'order_delivered'
  AND d.delivery_delay_hours IS NOT NULL
GROUP BY 1, 2
HAVING count(*) >= 10
ORDER BY on_time_pct ASC
LIMIT 5;
"""

LAST_ETL_RUN = """
SELECT
    run_id,
    max(run_ts) AS last_run_ts,
    sum(rows_in) AS total_rows_in,
    sum(rows_out) AS total_rows_out,
    bool_and(status = 'success') AS all_success
FROM mart.fact_data_quality
GROUP BY run_id
ORDER BY max(run_ts) DESC
LIMIT 1;
"""

REVIEW_SCORE_LAST6M = """
SELECT
    date_trunc('month', event_date)::date AS month,
    avg(review_score) AS avg_score
FROM mart.fact_reviews
GROUP BY 1
ORDER BY 1 DESC
LIMIT 6;
"""

DELIVERY_SLA_LAST6M = """
SELECT
    date_trunc('month', event_date)::date AS month,
    100.0 * sum(CASE WHEN delivery_delay_hours <= 0 THEN 1 ELSE 0 END)
        / NULLIF(count(*), 0) AS on_time_pct
FROM mart.fact_delivery_events
WHERE event_type = 'order_delivered'
  AND delivery_delay_hours IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC
LIMIT 6;
"""

ORDERS_LAST30D = """
SELECT
    event_date,
    count(DISTINCT order_id) AS orders
FROM mart.fact_order_events
WHERE event_type = 'order_created'
  AND event_date >= (SELECT max(event_date) - 30 FROM mart.fact_order_events)
GROUP BY 1
ORDER BY 1;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: DAILY SALES TREND
# ═══════════════════════════════════════════════════════════════════════════════

DAILY_SALES = """
SELECT
    event_date,
    count(DISTINCT order_id) AS orders,
    COALESCE(sum(payment_value), 0) AS revenue
FROM mart.fact_payments
WHERE payment_status = 'approved'
GROUP BY event_date
ORDER BY event_date;
"""

MONTHLY_SALES = """
SELECT
    date_trunc('month', event_date)::date AS month,
    count(DISTINCT order_id) AS orders,
    COALESCE(sum(payment_value), 0) AS revenue
FROM mart.fact_payments
WHERE payment_status = 'approved'
GROUP BY 1
ORDER BY 1;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: CATEGORY PERFORMANCE
# ═══════════════════════════════════════════════════════════════════════════════

CATEGORY_REVENUE = """
SELECT
    COALESCE(p.product_category_name, 'Unknown') AS category,
    count(DISTINCT i.order_id) AS orders,
    COALESCE(sum(i.price), 0) AS revenue,
    COALESCE(sum(i.freight_value), 0) AS freight
FROM mart.fact_order_items i
JOIN mart.dim_product p ON p.product_id = i.product_id
GROUP BY 1
ORDER BY revenue DESC
LIMIT 20;
"""

CATEGORY_AVG_REVIEW = """
SELECT
    COALESCE(p.product_category_name, 'Unknown') AS category,
    avg(r.review_score) AS avg_score,
    count(*) AS review_count
FROM mart.fact_reviews r
JOIN mart.fact_order_items i ON i.order_id = r.order_id
JOIN mart.dim_product p ON p.product_id = i.product_id
GROUP BY 1
HAVING count(*) >= 10
ORDER BY avg_score DESC
LIMIT 20;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: PAYMENT MIX
# ═══════════════════════════════════════════════════════════════════════════════

PAYMENT_MIX = """
SELECT
    payment_type,
    count(*) AS txn_count,
    COALESCE(sum(payment_value), 0) AS total_value,
    avg(payment_installments) AS avg_installments
FROM mart.fact_payments
WHERE payment_status = 'approved'
GROUP BY payment_type
ORDER BY total_value DESC;
"""

PAYMENT_TREND = """
SELECT
    date_trunc('month', event_date)::date AS month,
    payment_type,
    COALESCE(sum(payment_value), 0) AS total_value
FROM mart.fact_payments
WHERE payment_status = 'approved'
GROUP BY 1, 2
ORDER BY 1, 2;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5: DELIVERY SLA
# ═══════════════════════════════════════════════════════════════════════════════

DELIVERY_SLA_DISTRIBUTION = """
SELECT sla_bucket, sum(cnt) AS deliveries
FROM (
    SELECT
        CASE
            WHEN delivery_delay_hours IS NULL THEN 'No data'
            WHEN delivery_delay_hours <= -48 THEN '2+ days early'
            WHEN delivery_delay_hours <= -24 THEN '1-2 days early'
            WHEN delivery_delay_hours <= 0 THEN 'On time'
            WHEN delivery_delay_hours <= 24 THEN '1 day late'
            WHEN delivery_delay_hours <= 48 THEN '1-2 days late'
            ELSE '2+ days late'
        END AS sla_bucket,
        CASE
            WHEN delivery_delay_hours IS NULL THEN 6
            WHEN delivery_delay_hours <= -48 THEN 0
            WHEN delivery_delay_hours <= -24 THEN 1
            WHEN delivery_delay_hours <= 0 THEN 2
            WHEN delivery_delay_hours <= 24 THEN 3
            WHEN delivery_delay_hours <= 48 THEN 4
            ELSE 5
        END AS sort_order,
        1 AS cnt
    FROM mart.fact_delivery_events
    WHERE event_type = 'order_delivered'
) sub
GROUP BY sla_bucket, sort_order
ORDER BY sort_order;
"""

DELIVERY_BY_STATE = """
SELECT
    c.customer_state AS state,
    count(*) AS deliveries,
    avg(d.delivery_delay_hours) AS avg_delay_hours,
    100.0 * sum(CASE WHEN d.delivery_delay_hours <= 0 THEN 1 ELSE 0 END)
        / NULLIF(count(*), 0) AS on_time_pct
FROM mart.fact_delivery_events d
JOIN mart.dim_customer c ON c.customer_id = d.customer_id
WHERE d.event_type = 'order_delivered'
  AND d.delivery_delay_hours IS NOT NULL
GROUP BY 1
HAVING count(*) >= 10
ORDER BY avg_delay_hours DESC;
"""

DELIVERY_MONTHLY_TREND = """
SELECT
    date_trunc('month', event_date)::date AS month,
    avg(delivery_delay_hours) AS avg_delay_hours,
    100.0 * sum(CASE WHEN delivery_delay_hours <= 0 THEN 1 ELSE 0 END)
        / NULLIF(count(*), 0) AS on_time_pct
FROM mart.fact_delivery_events
WHERE event_type = 'order_delivered'
  AND delivery_delay_hours IS NOT NULL
GROUP BY 1
ORDER BY 1;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6: REVIEWS & SATISFACTION
# ═══════════════════════════════════════════════════════════════════════════════

REVIEW_SCORE_DISTRIBUTION = """
SELECT review_score, count(*) AS cnt
FROM mart.fact_reviews
GROUP BY review_score
ORDER BY review_score;
"""

REVIEW_MONTHLY_TREND = """
SELECT
    date_trunc('month', event_date)::date AS month,
    avg(review_score) AS avg_score,
    count(*) AS review_count
FROM mart.fact_reviews
GROUP BY 1
ORDER BY 1;
"""

REVIEW_RESPONSE_TIME = """
SELECT
    CASE
        WHEN review_response_hours IS NULL THEN 'No response'
        WHEN review_response_hours <= 24 THEN '< 24h'
        WHEN review_response_hours <= 48 THEN '24-48h'
        WHEN review_response_hours <= 72 THEN '48-72h'
        ELSE '72h+'
    END AS response_bucket,
    count(*) AS cnt
FROM mart.fact_reviews
GROUP BY 1;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7: SELLER PERFORMANCE
# ═══════════════════════════════════════════════════════════════════════════════

TOP_SELLERS_BY_REVENUE = """
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    count(DISTINCT i.order_id) AS orders,
    COALESCE(sum(i.price), 0) AS revenue
FROM mart.fact_order_items i
JOIN mart.dim_seller s ON s.seller_id = i.seller_id
GROUP BY 1, 2, 3
ORDER BY revenue DESC
LIMIT 20;
"""

SELLER_DELIVERY_PERFORMANCE = """
SELECT
    s.seller_id,
    s.seller_state,
    count(*) AS deliveries,
    avg(d.delivery_delay_hours) AS avg_delay_hours,
    100.0 * sum(CASE WHEN d.delivery_delay_hours <= 0 THEN 1 ELSE 0 END)
        / NULLIF(count(*), 0) AS on_time_pct
FROM mart.fact_delivery_events d
JOIN mart.dim_seller s ON s.seller_id = d.seller_id
WHERE d.event_type = 'order_delivered'
  AND d.delivery_delay_hours IS NOT NULL
GROUP BY 1, 2
HAVING count(*) >= 5
ORDER BY on_time_pct ASC
LIMIT 20;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8: GEOGRAPHY
# ═══════════════════════════════════════════════════════════════════════════════

ORDERS_BY_STATE = """
SELECT
    c.customer_state AS state,
    count(DISTINCT e.order_id) AS orders,
    count(DISTINCT c.customer_id) AS customers
FROM mart.fact_order_events e
JOIN mart.dim_customer c ON c.customer_id = e.customer_id
WHERE e.event_type = 'order_created'
GROUP BY 1
ORDER BY orders DESC;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9: OBSERVABILITY — Pipeline Health
# ═══════════════════════════════════════════════════════════════════════════════

DATA_QUALITY_RUNS = """
SELECT
    run_id,
    dataset_name,
    run_ts,
    rows_in,
    rows_out,
    null_violations,
    duplicate_violations,
    schema_violations,
    status
FROM mart.fact_data_quality
ORDER BY run_ts DESC
LIMIT 50;
"""

DATA_QUALITY_SUMMARY = """
SELECT
    dataset_name,
    count(*) AS total_runs,
    sum(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_runs,
    sum(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
    sum(rows_in) AS total_rows_in,
    sum(rows_out) AS total_rows_out,
    sum(null_violations) AS total_null_violations,
    sum(duplicate_violations) AS total_duplicate_violations,
    sum(schema_violations) AS total_schema_violations
FROM mart.fact_data_quality
GROUP BY dataset_name
ORDER BY dataset_name;
"""

INGESTION_FRESHNESS = """
SELECT
    'fact_order_events' AS table_name,
    max(ingestion_ts) AS last_ingestion,
    EXTRACT(EPOCH FROM (now() - max(ingestion_ts))) AS lag_seconds,
    count(*) AS total_rows
FROM mart.fact_order_events
UNION ALL
SELECT
    'fact_payments',
    max(ingestion_ts),
    EXTRACT(EPOCH FROM (now() - max(ingestion_ts))),
    count(*)
FROM mart.fact_payments
UNION ALL
SELECT
    'fact_delivery_events',
    max(ingestion_ts),
    EXTRACT(EPOCH FROM (now() - max(ingestion_ts))),
    count(*)
FROM mart.fact_delivery_events
UNION ALL
SELECT
    'fact_reviews',
    max(ingestion_ts),
    EXTRACT(EPOCH FROM (now() - max(ingestion_ts))),
    count(*)
FROM mart.fact_reviews
UNION ALL
SELECT
    'fact_order_items',
    max(event_date)::timestamp,
    EXTRACT(EPOCH FROM (now() - max(event_date)::timestamp)),
    count(*)
FROM mart.fact_order_items;
"""

TABLE_ROW_COUNTS = """
SELECT
    'dim_date' AS table_name, count(*) AS row_count FROM mart.dim_date
UNION ALL SELECT 'dim_customer', count(*) FROM mart.dim_customer
UNION ALL SELECT 'dim_seller', count(*) FROM mart.dim_seller
UNION ALL SELECT 'dim_product', count(*) FROM mart.dim_product
UNION ALL SELECT 'dim_payment_type', count(*) FROM mart.dim_payment_type
UNION ALL SELECT 'dim_event_type', count(*) FROM mart.dim_event_type
UNION ALL SELECT 'fact_order_events', count(*) FROM mart.fact_order_events
UNION ALL SELECT 'fact_payments', count(*) FROM mart.fact_payments
UNION ALL SELECT 'fact_delivery_events', count(*) FROM mart.fact_delivery_events
UNION ALL SELECT 'fact_reviews', count(*) FROM mart.fact_reviews
UNION ALL SELECT 'fact_order_items', count(*) FROM mart.fact_order_items
UNION ALL SELECT 'fact_data_quality', count(*) FROM mart.fact_data_quality;
"""

EVENT_VOLUME_BY_DAY = """
SELECT
    event_date,
    event_type,
    count(*) AS event_count
FROM mart.fact_order_events
GROUP BY 1, 2
ORDER BY 1, 2;
"""

INGESTION_RATE_HOURLY = """
SELECT
    date_trunc('hour', ingestion_ts) AS hour,
    count(*) AS events_ingested
FROM mart.fact_order_events
GROUP BY 1
ORDER BY 1 DESC
LIMIT 168;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10: DIM_DATE REPORTS (Calendar Intelligence)
# ═══════════════════════════════════════════════════════════════════════════════

WEEKDAY_VS_WEEKEND = """
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    count(DISTINCT e.order_id) AS orders,
    COALESCE(sum(p.payment_value), 0) AS revenue
FROM mart.fact_order_events e
JOIN mart.dim_date d ON d.full_date = e.event_date
LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
WHERE e.event_type = 'order_created'
GROUP BY 1;
"""

ORDERS_BY_DAY_OF_WEEK = """
SELECT
    d.day_name,
    d.day_of_week,
    count(DISTINCT e.order_id) AS orders
FROM mart.fact_order_events e
JOIN mart.dim_date d ON d.full_date = e.event_date
WHERE e.event_type = 'order_created'
GROUP BY 1, 2
ORDER BY 2;
"""

QUARTERLY_PERFORMANCE = """
SELECT
    d.year,
    d.quarter,
    count(DISTINCT e.order_id) AS orders,
    COALESCE(sum(p.payment_value), 0) AS revenue
FROM mart.fact_order_events e
JOIN mart.dim_date d ON d.full_date = e.event_date
LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
WHERE e.event_type = 'order_created'
GROUP BY 1, 2
ORDER BY 1, 2;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 11: DIM_EVENT_TYPE & DIM_PAYMENT_TYPE ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════════

EVENT_TYPE_DOMAIN_SUMMARY = """
SELECT
    et.domain,
    et.event_type,
    et.is_terminal_state,
    COALESCE(cnt.total, 0) AS event_count
FROM mart.dim_event_type et
LEFT JOIN (
    SELECT event_type, count(*) AS total FROM mart.fact_order_events GROUP BY 1
) cnt ON cnt.event_type = et.event_type
ORDER BY et.domain, et.is_terminal_state, event_count DESC;
"""

PAYMENT_TYPE_ANALYSIS = """
SELECT
    pt.payment_type,
    pt.is_digital,
    pt.is_installment_supported,
    count(*) AS txn_count,
    COALESCE(sum(fp.payment_value), 0) AS total_value,
    avg(fp.payment_installments) AS avg_installments
FROM mart.fact_payments fp
JOIN mart.dim_payment_type pt ON pt.payment_type = fp.payment_type
WHERE fp.payment_status = 'approved'
GROUP BY 1, 2, 3
ORDER BY total_value DESC;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 12: ADVANCED OBSERVABILITY
# ═══════════════════════════════════════════════════════════════════════════════

PIPELINE_THROUGHPUT = """
SELECT
    date_trunc('day', ingestion_ts)::date AS day,
    count(*) AS events_ingested,
    min(event_ts) AS earliest_event,
    max(event_ts) AS latest_event,
    avg(EXTRACT(EPOCH FROM (ingestion_ts - event_ts))) AS avg_lag_seconds
FROM mart.fact_order_events
WHERE ingestion_ts IS NOT NULL
GROUP BY 1
ORDER BY 1;
"""

DATA_COMPLETENESS = """
SELECT
    'fact_order_events' AS table_name,
    count(*) AS total_rows,
    sum(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer,
    sum(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller,
    sum(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) AS null_status
FROM mart.fact_order_events
UNION ALL
SELECT
    'fact_payments',
    count(*),
    sum(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
    0,
    sum(CASE WHEN payment_status IS NULL THEN 1 ELSE 0 END)
FROM mart.fact_payments
UNION ALL
SELECT
    'fact_delivery_events',
    count(*),
    sum(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
    sum(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END),
    sum(CASE WHEN carrier_status IS NULL THEN 1 ELSE 0 END)
FROM mart.fact_delivery_events;
"""

DUPLICATE_CHECK = """
SELECT
    'fact_order_events' AS table_name,
    count(*) AS total,
    count(DISTINCT event_id) AS distinct_events,
    count(*) - count(DISTINCT event_id) AS duplicates
FROM mart.fact_order_events
UNION ALL
SELECT 'fact_payments', count(*), count(DISTINCT event_id),
    count(*) - count(DISTINCT event_id) FROM mart.fact_payments
UNION ALL
SELECT 'fact_delivery_events', count(*), count(DISTINCT event_id),
    count(*) - count(DISTINCT event_id) FROM mart.fact_delivery_events
UNION ALL
SELECT 'fact_reviews', count(*), count(DISTINCT event_id),
    count(*) - count(DISTINCT event_id) FROM mart.fact_reviews;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 13: CUSTOMER SEGMENTATION
# ═══════════════════════════════════════════════════════════════════════════════

CUSTOMER_RFM = """
WITH customer_orders AS (
    SELECT
        customer_id,
        count(DISTINCT order_id) AS frequency,
        max(event_date) AS last_order_date,
        (SELECT max(event_date) FROM mart.fact_order_events WHERE event_type='order_created') AS ref_date
    FROM mart.fact_order_events
    WHERE event_type = 'order_created'
    GROUP BY 1
),
customer_monetary AS (
    SELECT
        e.customer_id,
        COALESCE(sum(p.payment_value), 0) AS monetary
    FROM mart.fact_order_events e
    JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
    WHERE e.event_type = 'order_created'
    GROUP BY 1
),
rfm AS (
    SELECT
        co.customer_id,
        (co.ref_date - co.last_order_date) AS recency_days,
        co.frequency,
        COALESCE(cm.monetary, 0) AS monetary
    FROM customer_orders co
    LEFT JOIN customer_monetary cm ON cm.customer_id = co.customer_id
)
SELECT
    CASE
        WHEN recency_days <= 30 AND frequency >= 3 AND monetary >= 500 THEN 'Champions'
        WHEN recency_days <= 60 AND frequency >= 2 THEN 'Loyal'
        WHEN recency_days <= 30 THEN 'Recent'
        WHEN recency_days <= 90 AND frequency >= 2 THEN 'At Risk'
        WHEN recency_days > 180 THEN 'Lost'
        ELSE 'Casual'
    END AS segment,
    count(*) AS customers,
    avg(recency_days) AS avg_recency,
    avg(frequency) AS avg_frequency,
    avg(monetary) AS avg_monetary
FROM rfm
GROUP BY 1
ORDER BY avg_monetary DESC;
"""

CUSTOMER_REPEAT_VS_ONETIME = """
SELECT
    CASE WHEN order_count = 1 THEN 'One-time' ELSE 'Repeat' END AS customer_type,
    count(*) AS customers,
    sum(revenue) AS total_revenue
FROM (
    SELECT
        e.customer_id,
        count(DISTINCT e.order_id) AS order_count,
        COALESCE(sum(p.payment_value), 0) AS revenue
    FROM mart.fact_order_events e
    LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
    WHERE e.event_type = 'order_created'
    GROUP BY 1
) sub
GROUP BY 1;
"""

CUSTOMER_CLV_DISTRIBUTION = """
SELECT
    CASE
        WHEN clv < 50 THEN '< R$50'
        WHEN clv < 100 THEN 'R$50-100'
        WHEN clv < 200 THEN 'R$100-200'
        WHEN clv < 500 THEN 'R$200-500'
        WHEN clv < 1000 THEN 'R$500-1000'
        ELSE 'R$1000+'
    END AS clv_bucket,
    count(*) AS customers
FROM (
    SELECT
        e.customer_id,
        COALESCE(sum(p.payment_value), 0) AS clv
    FROM mart.fact_order_events e
    LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
    WHERE e.event_type = 'order_created'
    GROUP BY 1
) sub
GROUP BY 1
ORDER BY min(clv);
"""

CUSTOMER_ACQUISITION_TREND = """
SELECT
    date_trunc('month', first_order)::date AS cohort_month,
    count(*) AS new_customers
FROM (
    SELECT
        customer_id,
        min(event_date) AS first_order
    FROM mart.fact_order_events
    WHERE event_type = 'order_created'
    GROUP BY 1
) sub
GROUP BY 1
ORDER BY 1;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 14: PRODUCT PERFORMANCE
# ═══════════════════════════════════════════════════════════════════════════════

PRODUCT_TOP_BY_REVENUE = """
SELECT
    p.product_id,
    COALESCE(p.product_category_name, 'Unknown') AS category,
    count(*) AS units_sold,
    COALESCE(sum(i.price), 0) AS revenue
FROM mart.fact_order_items i
JOIN mart.dim_product p ON p.product_id = i.product_id
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 20;
"""

PRODUCT_BOTTOM_BY_REVENUE = """
SELECT
    p.product_id,
    COALESCE(p.product_category_name, 'Unknown') AS category,
    count(*) AS units_sold,
    COALESCE(sum(i.price), 0) AS revenue
FROM mart.fact_order_items i
JOIN mart.dim_product p ON p.product_id = i.product_id
GROUP BY 1, 2
HAVING count(*) >= 5
ORDER BY revenue ASC
LIMIT 20;
"""

PRODUCT_CATEGORY_HEATMAP = """
SELECT
    COALESCE(p.product_category_name, 'Unknown') AS category,
    date_trunc('month', i.event_date)::date AS month,
    COALESCE(sum(i.price), 0) AS revenue
FROM mart.fact_order_items i
JOIN mart.dim_product p ON p.product_id = i.product_id
GROUP BY 1, 2
ORDER BY 2, 1;
"""

PRODUCT_PRICE_DISTRIBUTION = """
SELECT
    CASE
        WHEN price < 50 THEN '< R$50'
        WHEN price < 100 THEN 'R$50-100'
        WHEN price < 200 THEN 'R$100-200'
        WHEN price < 500 THEN 'R$200-500'
        WHEN price < 1000 THEN 'R$500-1000'
        ELSE 'R$1000+'
    END AS price_bucket,
    count(*) AS items
FROM mart.fact_order_items
GROUP BY 1
ORDER BY min(price);
"""

ITEMS_PER_ORDER_DISTRIBUTION = """
SELECT
    items_in_order,
    count(*) AS order_count
FROM (
    SELECT order_id, count(*) AS items_in_order
    FROM mart.fact_order_items
    GROUP BY 1
) sub
GROUP BY 1
ORDER BY 1;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 15: GEOGRAPHIC ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

REVENUE_BY_STATE = """
SELECT
    c.customer_state AS state,
    count(DISTINCT e.order_id) AS orders,
    COALESCE(sum(p.payment_value), 0) AS revenue,
    count(DISTINCT c.customer_id) AS customers
FROM mart.fact_order_events e
JOIN mart.dim_customer c ON c.customer_id = e.customer_id
LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
WHERE e.event_type = 'order_created'
GROUP BY 1
ORDER BY revenue DESC;
"""

TOP_CITIES_BY_ORDERS = """
SELECT
    c.customer_city AS city,
    c.customer_state AS state,
    count(DISTINCT e.order_id) AS orders,
    COALESCE(sum(p.payment_value), 0) AS revenue
FROM mart.fact_order_events e
JOIN mart.dim_customer c ON c.customer_id = e.customer_id
LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
WHERE e.event_type = 'order_created'
GROUP BY 1, 2
ORDER BY orders DESC
LIMIT 20;
"""

DELIVERY_BY_ROUTE = """
SELECT
    s.seller_state AS origin_state,
    c.customer_state AS dest_state,
    count(*) AS deliveries,
    avg(d.delivery_delay_hours) AS avg_delay_hours,
    100.0 * sum(CASE WHEN d.delivery_delay_hours <= 0 THEN 1 ELSE 0 END)
        / NULLIF(count(*), 0) AS on_time_pct
FROM mart.fact_delivery_events d
JOIN mart.dim_seller s ON s.seller_id = d.seller_id
JOIN mart.dim_customer c ON c.customer_id = d.customer_id
WHERE d.event_type = 'order_delivered'
  AND d.delivery_delay_hours IS NOT NULL
GROUP BY 1, 2
HAVING count(*) >= 10
ORDER BY deliveries DESC
LIMIT 30;
"""

REGIONAL_GROWTH_TREND = """
SELECT
    c.customer_state AS state,
    date_trunc('month', e.event_date)::date AS month,
    count(DISTINCT e.order_id) AS orders
FROM mart.fact_order_events e
JOIN mart.dim_customer c ON c.customer_id = e.customer_id
WHERE e.event_type = 'order_created'
GROUP BY 1, 2
ORDER BY 2, 1;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 16: ORDER FUNNEL / LIFECYCLE
# ═══════════════════════════════════════════════════════════════════════════════

ORDER_STATUS_BREAKDOWN = """
SELECT
    order_status,
    count(DISTINCT order_id) AS orders
FROM mart.fact_order_events
GROUP BY 1
ORDER BY orders DESC;
"""

ORDER_LIFECYCLE_TIMES = """
SELECT
    avg(EXTRACT(EPOCH FROM (approved_ts - purchase_ts)) / 3600) AS avg_approval_hours,
    avg(EXTRACT(EPOCH FROM (d.shipped_ts - e.approved_ts)) / 3600) AS avg_ship_hours,
    avg(EXTRACT(EPOCH FROM (d.delivered_customer_ts - d.shipped_ts)) / 3600) AS avg_transit_hours,
    avg(EXTRACT(EPOCH FROM (d.delivered_customer_ts - e.purchase_ts)) / 86400) AS avg_total_days
FROM mart.fact_order_events e
JOIN mart.fact_delivery_events d ON d.order_id = e.order_id AND d.event_type = 'order_delivered'
WHERE e.event_type = 'order_created'
  AND e.approved_ts IS NOT NULL
  AND d.shipped_ts IS NOT NULL
  AND d.delivered_customer_ts IS NOT NULL;
"""

CANCELLATION_RATE_TREND = """
SELECT
    date_trunc('month', event_date)::date AS month,
    count(DISTINCT order_id) AS total_orders,
    count(DISTINCT CASE WHEN order_status = 'canceled' THEN order_id END) AS cancelled,
    100.0 * count(DISTINCT CASE WHEN order_status = 'canceled' THEN order_id END)
        / NULLIF(count(DISTINCT order_id), 0) AS cancel_pct
FROM mart.fact_order_events
GROUP BY 1
ORDER BY 1;
"""

ORDER_LEAD_TIME_DISTRIBUTION = """
SELECT
    CASE
        WHEN lead_days <= 3 THEN '0-3 days'
        WHEN lead_days <= 7 THEN '4-7 days'
        WHEN lead_days <= 14 THEN '8-14 days'
        WHEN lead_days <= 21 THEN '15-21 days'
        WHEN lead_days <= 30 THEN '22-30 days'
        ELSE '30+ days'
    END AS lead_bucket,
    count(*) AS orders
FROM (
    SELECT
        EXTRACT(EPOCH FROM (d.delivered_customer_ts - e.purchase_ts)) / 86400 AS lead_days
    FROM mart.fact_order_events e
    JOIN mart.fact_delivery_events d ON d.order_id = e.order_id AND d.event_type = 'order_delivered'
    WHERE e.event_type = 'order_created'
      AND d.delivered_customer_ts IS NOT NULL
      AND e.purchase_ts IS NOT NULL
) sub
WHERE lead_days > 0
GROUP BY 1
ORDER BY min(lead_days);
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 17: REVENUE COHORT ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

CUSTOMER_COHORTS = """
WITH first_purchase AS (
    SELECT customer_id, min(event_date) AS first_date
    FROM mart.fact_order_events
    WHERE event_type = 'order_created'
    GROUP BY 1
),
cohort_data AS (
    SELECT
        date_trunc('month', fp.first_date)::date AS cohort_month,
        (EXTRACT(YEAR FROM e.event_date) - EXTRACT(YEAR FROM fp.first_date)) * 12
            + (EXTRACT(MONTH FROM e.event_date) - EXTRACT(MONTH FROM fp.first_date)) AS month_offset,
        count(DISTINCT e.customer_id) AS active_customers
    FROM mart.fact_order_events e
    JOIN first_purchase fp ON fp.customer_id = e.customer_id
    WHERE e.event_type = 'order_created'
    GROUP BY 1, 2
)
SELECT cohort_month, month_offset, active_customers
FROM cohort_data
WHERE month_offset <= 12
ORDER BY 1, 2;
"""

COHORT_REVENUE = """
WITH first_purchase AS (
    SELECT customer_id, min(event_date) AS first_date
    FROM mart.fact_order_events
    WHERE event_type = 'order_created'
    GROUP BY 1
)
SELECT
    date_trunc('month', fp.first_date)::date AS cohort_month,
    (EXTRACT(YEAR FROM p.event_date) - EXTRACT(YEAR FROM fp.first_date)) * 12
        + (EXTRACT(MONTH FROM p.event_date) - EXTRACT(MONTH FROM fp.first_date)) AS month_offset,
    COALESCE(sum(p.payment_value), 0) AS revenue,
    count(DISTINCT p.order_id) AS orders
FROM mart.fact_payments p
JOIN mart.fact_order_events e ON e.order_id = p.order_id AND e.event_type = 'order_created'
JOIN first_purchase fp ON fp.customer_id = e.customer_id
WHERE p.payment_status = 'approved'
GROUP BY 1, 2
HAVING (EXTRACT(YEAR FROM p.event_date) - EXTRACT(YEAR FROM fp.first_date)) * 12
    + (EXTRACT(MONTH FROM p.event_date) - EXTRACT(MONTH FROM fp.first_date)) <= 12
ORDER BY 1, 2;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 18: SELLER ANALYTICS (EXPANDED)
# ═══════════════════════════════════════════════════════════════════════════════

SELLER_CONCENTRATION = """
WITH seller_rev AS (
    SELECT
        i.seller_id,
        COALESCE(sum(i.price), 0) AS revenue
    FROM mart.fact_order_items i
    GROUP BY 1
),
ranked AS (
    SELECT
        seller_id,
        revenue,
        sum(revenue) OVER (ORDER BY revenue DESC) AS cumulative_revenue,
        sum(revenue) OVER () AS total_revenue,
        row_number() OVER (ORDER BY revenue DESC) AS rn,
        count(*) OVER () AS total_sellers
    FROM seller_rev
)
SELECT
    rn AS seller_rank,
    seller_id,
    revenue,
    100.0 * cumulative_revenue / NULLIF(total_revenue, 0) AS cumulative_pct,
    100.0 * rn / NULLIF(total_sellers, 0) AS seller_pct
FROM ranked
ORDER BY rn
LIMIT 50;
"""

SELLER_CATEGORY_SPECIALIZATION = """
SELECT
    s.seller_id,
    s.seller_state,
    COALESCE(p.product_category_name, 'Unknown') AS primary_category,
    cat_revenue,
    total_revenue,
    100.0 * cat_revenue / NULLIF(total_revenue, 0) AS specialization_pct
FROM (
    SELECT
        i.seller_id,
        p.product_category_name,
        sum(i.price) AS cat_revenue,
        sum(sum(i.price)) OVER (PARTITION BY i.seller_id) AS total_revenue,
        row_number() OVER (PARTITION BY i.seller_id ORDER BY sum(i.price) DESC) AS rn
    FROM mart.fact_order_items i
    JOIN mart.dim_product p ON p.product_id = i.product_id
    GROUP BY i.seller_id, p.product_category_name
) sub
JOIN mart.dim_seller s ON s.seller_id = sub.seller_id
JOIN mart.dim_product p ON p.product_category_name = sub.product_category_name
WHERE sub.rn = 1 AND total_revenue >= 1000
GROUP BY 1, 2, 3, 4, 5
ORDER BY specialization_pct DESC
LIMIT 30;
"""

SELLER_RATING_VS_DELIVERY = """
SELECT
    s.seller_id,
    s.seller_state,
    avg(r.review_score) AS avg_rating,
    avg(d.delivery_delay_hours) AS avg_delay_hours,
    count(DISTINCT d.order_id) AS deliveries
FROM mart.dim_seller s
JOIN mart.fact_delivery_events d ON d.seller_id = s.seller_id AND d.event_type = 'order_delivered'
JOIN mart.fact_reviews r ON r.order_id = d.order_id
WHERE d.delivery_delay_hours IS NOT NULL
GROUP BY 1, 2
HAVING count(DISTINCT d.order_id) >= 10
ORDER BY avg_rating DESC
LIMIT 50;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 19: PAYMENT INSTALLMENTS
# ═══════════════════════════════════════════════════════════════════════════════

INSTALLMENT_DISTRIBUTION = """
SELECT
    payment_installments,
    count(*) AS txn_count,
    avg(payment_value) AS avg_value,
    sum(payment_value) AS total_value
FROM mart.fact_payments
WHERE payment_status = 'approved'
GROUP BY 1
ORDER BY 1;
"""

INSTALLMENT_AOV = """
SELECT
    payment_installments,
    avg(order_total) AS avg_order_value,
    count(*) AS orders
FROM (
    SELECT
        order_id,
        max(payment_installments) AS payment_installments,
        sum(payment_value) AS order_total
    FROM mart.fact_payments
    WHERE payment_status = 'approved'
    GROUP BY 1
) sub
GROUP BY 1
ORDER BY 1;
"""

INSTALLMENT_TREND = """
SELECT
    date_trunc('month', event_date)::date AS month,
    avg(payment_installments) AS avg_installments,
    100.0 * sum(CASE WHEN payment_installments > 1 THEN 1 ELSE 0 END)
        / NULLIF(count(*), 0) AS pct_installment_usage
FROM mart.fact_payments
WHERE payment_status = 'approved'
GROUP BY 1
ORDER BY 1;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 20: SEASONALITY / CALENDAR
# ═══════════════════════════════════════════════════════════════════════════════

ORDERS_BY_MONTH_NAME = """
SELECT
    d.month AS month_num,
    d.month_name,
    count(DISTINCT e.order_id) AS orders,
    COALESCE(sum(p.payment_value), 0) AS revenue
FROM mart.fact_order_events e
JOIN mart.dim_date d ON d.full_date = e.event_date
LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
WHERE e.event_type = 'order_created'
GROUP BY 1, 2
ORDER BY 1;
"""

ORDERS_BY_QUARTER_YOY = """
SELECT
    d.year,
    d.quarter,
    d.year || '-Q' || d.quarter AS period,
    count(DISTINCT e.order_id) AS orders,
    COALESCE(sum(p.payment_value), 0) AS revenue
FROM mart.fact_order_events e
JOIN mart.dim_date d ON d.full_date = e.event_date
LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
WHERE e.event_type = 'order_created'
GROUP BY 1, 2, 3
ORDER BY 1, 2;
"""

WEEKDAY_HOURLY_PATTERN = """
SELECT
    d.day_name,
    d.day_of_week,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    count(DISTINCT e.order_id) AS orders,
    avg(p.payment_value) AS avg_order_value
FROM mart.fact_order_events e
JOIN mart.dim_date d ON d.full_date = e.event_date
LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
WHERE e.event_type = 'order_created'
GROUP BY 1, 2, 3
ORDER BY 2;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 21: BASKET ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

BASKET_SIZE_TREND = """
SELECT
    date_trunc('month', event_date)::date AS month,
    avg(items_in_order) AS avg_basket_size,
    avg(order_total) AS avg_basket_value
FROM (
    SELECT
        order_id,
        min(event_date) AS event_date,
        count(*) AS items_in_order,
        sum(price) AS order_total
    FROM mart.fact_order_items
    GROUP BY 1
) sub
GROUP BY 1
ORDER BY 1;
"""

MULTI_ITEM_ORDERS_PCT = """
SELECT
    CASE WHEN items > 1 THEN 'Multi-item' ELSE 'Single-item' END AS order_type,
    count(*) AS orders,
    avg(total) AS avg_value
FROM (
    SELECT order_id, count(*) AS items, sum(price) AS total
    FROM mart.fact_order_items
    GROUP BY 1
) sub
GROUP BY 1;
"""

TOP_CATEGORY_COMBINATIONS = """
SELECT
    c1.category AS category_1,
    c2.category AS category_2,
    count(*) AS co_occurrence
FROM (
    SELECT i.order_id, COALESCE(p.product_category_name, 'Unknown') AS category
    FROM mart.fact_order_items i
    JOIN mart.dim_product p ON p.product_id = i.product_id
) c1
JOIN (
    SELECT i.order_id, COALESCE(p.product_category_name, 'Unknown') AS category
    FROM mart.fact_order_items i
    JOIN mart.dim_product p ON p.product_id = i.product_id
) c2 ON c1.order_id = c2.order_id AND c1.category < c2.category
GROUP BY 1, 2
HAVING count(*) >= 10
ORDER BY co_occurrence DESC
LIMIT 20;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 22: FREIGHT & LOGISTICS
# ═══════════════════════════════════════════════════════════════════════════════

FREIGHT_COST_DISTRIBUTION = """
SELECT
    CASE
        WHEN freight_value < 10 THEN '< R$10'
        WHEN freight_value < 20 THEN 'R$10-20'
        WHEN freight_value < 30 THEN 'R$20-30'
        WHEN freight_value < 50 THEN 'R$30-50'
        WHEN freight_value < 100 THEN 'R$50-100'
        ELSE 'R$100+'
    END AS freight_bucket,
    count(*) AS items
FROM mart.fact_order_items
GROUP BY 1
ORDER BY min(freight_value);
"""

FREIGHT_PCT_OF_ORDER = """
SELECT
    CASE
        WHEN freight_pct < 5 THEN '< 5%'
        WHEN freight_pct < 10 THEN '5-10%'
        WHEN freight_pct < 20 THEN '10-20%'
        WHEN freight_pct < 30 THEN '20-30%'
        WHEN freight_pct < 50 THEN '30-50%'
        ELSE '50%+'
    END AS freight_pct_bucket,
    count(*) AS orders
FROM (
    SELECT
        order_id,
        100.0 * sum(freight_value) / NULLIF(sum(price + freight_value), 0) AS freight_pct
    FROM mart.fact_order_items
    GROUP BY 1
) sub
GROUP BY 1
ORDER BY min(freight_pct);
"""

FREIGHT_BY_STATE = """
SELECT
    c.customer_state AS state,
    count(*) AS items,
    avg(i.freight_value) AS avg_freight,
    sum(i.freight_value) AS total_freight,
    avg(i.price) AS avg_price,
    100.0 * sum(i.freight_value) / NULLIF(sum(i.price + i.freight_value), 0) AS freight_pct
FROM mart.fact_order_items i
JOIN mart.fact_order_events e ON e.order_id = i.order_id AND e.event_type = 'order_created'
JOIN mart.dim_customer c ON c.customer_id = e.customer_id
GROUP BY 1
HAVING count(*) >= 10
ORDER BY avg_freight DESC;
"""

ESTIMATED_VS_ACTUAL_DELIVERY = """
SELECT
    date_trunc('month', d.event_date)::date AS month,
    avg(EXTRACT(EPOCH FROM (d.delivered_customer_ts - e.estimated_delivery_ts)) / 86400) AS avg_diff_days,
    100.0 * sum(CASE WHEN d.delivered_customer_ts <= e.estimated_delivery_ts THEN 1 ELSE 0 END)
        / NULLIF(count(*), 0) AS met_estimate_pct,
    count(*) AS deliveries
FROM mart.fact_delivery_events d
JOIN mart.fact_order_events e ON e.order_id = d.order_id AND e.event_type = 'order_created'
WHERE d.event_type = 'order_delivered'
  AND d.delivered_customer_ts IS NOT NULL
  AND e.estimated_delivery_ts IS NOT NULL
GROUP BY 1
ORDER BY 1;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 23: CORRELATION ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

CORR_REVIEW_VS_DELAY = """
SELECT
    CASE
        WHEN d.delivery_delay_hours IS NULL THEN 'No delivery data'
        WHEN d.delivery_delay_hours <= -48 THEN '2+ days early'
        WHEN d.delivery_delay_hours <= -24 THEN '1-2 days early'
        WHEN d.delivery_delay_hours <= 0 THEN 'On time'
        WHEN d.delivery_delay_hours <= 24 THEN '1 day late'
        WHEN d.delivery_delay_hours <= 48 THEN '1-2 days late'
        ELSE '2+ days late'
    END AS delay_bucket,
    avg(r.review_score) AS avg_score,
    count(*) AS reviews
FROM mart.fact_reviews r
JOIN mart.fact_delivery_events d ON d.order_id = r.order_id AND d.event_type = 'order_delivered'
GROUP BY 1
ORDER BY avg_score DESC;
"""

CORR_PRICE_VS_REVIEW = """
SELECT
    CASE
        WHEN i.price < 50 THEN '< R$50'
        WHEN i.price < 100 THEN 'R$50-100'
        WHEN i.price < 200 THEN 'R$100-200'
        WHEN i.price < 500 THEN 'R$200-500'
        ELSE 'R$500+'
    END AS price_bucket,
    avg(r.review_score) AS avg_score,
    count(*) AS reviews
FROM mart.fact_reviews r
JOIN mart.fact_order_items i ON i.order_id = r.order_id
GROUP BY 1
ORDER BY min(i.price);
"""

CORR_FREIGHT_VS_REVIEW = """
SELECT
    CASE
        WHEN i.freight_value < 10 THEN '< R$10'
        WHEN i.freight_value < 20 THEN 'R$10-20'
        WHEN i.freight_value < 30 THEN 'R$20-30'
        WHEN i.freight_value < 50 THEN 'R$30-50'
        ELSE 'R$50+'
    END AS freight_bucket,
    avg(r.review_score) AS avg_score,
    count(*) AS reviews
FROM mart.fact_reviews r
JOIN mart.fact_order_items i ON i.order_id = r.order_id
GROUP BY 1
ORDER BY min(i.freight_value);
"""

CORR_PAYMENT_METHOD_VS_REVIEW = """
SELECT
    p.payment_type,
    avg(r.review_score) AS avg_score,
    count(*) AS reviews
FROM mart.fact_reviews r
JOIN mart.fact_payments p ON p.order_id = r.order_id AND p.payment_status = 'approved'
GROUP BY 1
ORDER BY avg_score DESC;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 24: ANOMALY DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

ANOMALY_DAILY_ORDERS = """
WITH daily AS (
    SELECT
        event_date,
        count(DISTINCT order_id) AS orders
    FROM mart.fact_order_events
    WHERE event_type = 'order_created'
    GROUP BY 1
),
stats AS (
    SELECT
        avg(orders) AS mean_orders,
        stddev(orders) AS std_orders
    FROM daily
)
SELECT
    d.event_date,
    d.orders,
    s.mean_orders,
    s.std_orders,
    (d.orders - s.mean_orders) / NULLIF(s.std_orders, 0) AS z_score
FROM daily d
CROSS JOIN stats s
ORDER BY d.event_date;
"""

ANOMALY_DAILY_REVENUE = """
WITH daily AS (
    SELECT
        event_date,
        sum(payment_value) AS revenue
    FROM mart.fact_payments
    WHERE payment_status = 'approved'
    GROUP BY 1
),
stats AS (
    SELECT
        avg(revenue) AS mean_rev,
        stddev(revenue) AS std_rev
    FROM daily
)
SELECT
    d.event_date,
    d.revenue,
    s.mean_rev,
    s.std_rev,
    (d.revenue - s.mean_rev) / NULLIF(s.std_rev, 0) AS z_score
FROM daily d
CROSS JOIN stats s
ORDER BY d.event_date;
"""

ANOMALY_DELIVERY_DELAY = """
WITH monthly AS (
    SELECT
        date_trunc('month', event_date)::date AS month,
        avg(delivery_delay_hours) AS avg_delay
    FROM mart.fact_delivery_events
    WHERE event_type = 'order_delivered'
      AND delivery_delay_hours IS NOT NULL
    GROUP BY 1
),
stats AS (
    SELECT avg(avg_delay) AS mean_delay, stddev(avg_delay) AS std_delay FROM monthly
)
SELECT
    m.month,
    m.avg_delay,
    s.mean_delay,
    (m.avg_delay - s.mean_delay) / NULLIF(s.std_delay, 0) AS z_score
FROM monthly m
CROSS JOIN stats s
ORDER BY m.month;
"""

ANOMALY_CATEGORY_SURGE = """
WITH monthly_cat AS (
    SELECT
        COALESCE(p.product_category_name, 'Unknown') AS category,
        date_trunc('month', i.event_date)::date AS month,
        sum(i.price) AS revenue
    FROM mart.fact_order_items i
    JOIN mart.dim_product p ON p.product_id = i.product_id
    GROUP BY 1, 2
),
cat_stats AS (
    SELECT
        category,
        avg(revenue) AS mean_rev,
        stddev(revenue) AS std_rev
    FROM monthly_cat
    GROUP BY 1
    HAVING stddev(revenue) > 0
)
SELECT
    mc.category,
    mc.month,
    mc.revenue,
    cs.mean_rev,
    (mc.revenue - cs.mean_rev) / cs.std_rev AS z_score
FROM monthly_cat mc
JOIN cat_stats cs ON cs.category = mc.category
WHERE abs((mc.revenue - cs.mean_rev) / cs.std_rev) > 2
ORDER BY abs((mc.revenue - cs.mean_rev) / cs.std_rev) DESC
LIMIT 30;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 25: SELLER CHURN & GROWTH
# ═══════════════════════════════════════════════════════════════════════════════

SELLER_ONBOARDING_TREND = """
SELECT
    date_trunc('month', first_sale)::date AS onboard_month,
    count(*) AS new_sellers
FROM (
    SELECT seller_id, min(event_date) AS first_sale
    FROM mart.fact_order_items
    GROUP BY 1
) sub
GROUP BY 1
ORDER BY 1;
"""

SELLER_INACTIVE = """
WITH seller_last AS (
    SELECT seller_id, max(event_date) AS last_sale
    FROM mart.fact_order_items
    GROUP BY 1
),
max_date AS (SELECT max(event_date) AS ref FROM mart.fact_order_items)
SELECT
    CASE
        WHEN (m.ref - sl.last_sale) <= 30 THEN 'Active (≤30d)'
        WHEN (m.ref - sl.last_sale) <= 60 THEN 'Slowing (31-60d)'
        WHEN (m.ref - sl.last_sale) <= 90 THEN 'At Risk (61-90d)'
        ELSE 'Inactive (90d+)'
    END AS status,
    count(*) AS sellers
FROM seller_last sl
CROSS JOIN max_date m
GROUP BY 1
ORDER BY min(m.ref - sl.last_sale);
"""

SELLER_REVENUE_GROWTH = """
WITH monthly_seller AS (
    SELECT
        seller_id,
        date_trunc('month', event_date)::date AS month,
        sum(price) AS revenue
    FROM mart.fact_order_items
    GROUP BY 1, 2
),
last_two AS (
    SELECT
        seller_id,
        month,
        revenue,
        lag(revenue) OVER (PARTITION BY seller_id ORDER BY month) AS prev_revenue,
        row_number() OVER (PARTITION BY seller_id ORDER BY month DESC) AS rn
    FROM monthly_seller
)
SELECT
    seller_id,
    month,
    revenue AS current_revenue,
    prev_revenue,
    CASE WHEN prev_revenue > 0
        THEN 100.0 * (revenue - prev_revenue) / prev_revenue
        ELSE NULL
    END AS growth_pct
FROM last_two
WHERE rn = 1 AND prev_revenue IS NOT NULL
ORDER BY growth_pct DESC
LIMIT 20;
"""

SELLER_REVENUE_DECLINE = """
WITH monthly_seller AS (
    SELECT
        seller_id,
        date_trunc('month', event_date)::date AS month,
        sum(price) AS revenue
    FROM mart.fact_order_items
    GROUP BY 1, 2
),
last_two AS (
    SELECT
        seller_id,
        month,
        revenue,
        lag(revenue) OVER (PARTITION BY seller_id ORDER BY month) AS prev_revenue,
        row_number() OVER (PARTITION BY seller_id ORDER BY month DESC) AS rn
    FROM monthly_seller
)
SELECT
    seller_id,
    month,
    revenue AS current_revenue,
    prev_revenue,
    CASE WHEN prev_revenue > 0
        THEN 100.0 * (revenue - prev_revenue) / prev_revenue
        ELSE NULL
    END AS growth_pct
FROM last_two
WHERE rn = 1 AND prev_revenue IS NOT NULL
ORDER BY growth_pct ASC
LIMIT 20;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 26: CUSTOMER JOURNEY ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

CUSTOMER_TIME_TO_SECOND_PURCHASE = """
WITH ordered AS (
    SELECT
        customer_id,
        event_date,
        row_number() OVER (PARTITION BY customer_id ORDER BY event_date) AS purchase_num
    FROM mart.fact_order_events
    WHERE event_type = 'order_created'
)
SELECT
    CASE
        WHEN days_to_second <= 7 THEN '0-7 days'
        WHEN days_to_second <= 14 THEN '8-14 days'
        WHEN days_to_second <= 30 THEN '15-30 days'
        WHEN days_to_second <= 60 THEN '31-60 days'
        WHEN days_to_second <= 90 THEN '61-90 days'
        ELSE '90+ days'
    END AS bucket,
    count(*) AS customers
FROM (
    SELECT
        o1.customer_id,
        o2.event_date - o1.event_date AS days_to_second
    FROM ordered o1
    JOIN ordered o2 ON o2.customer_id = o1.customer_id AND o2.purchase_num = 2
    WHERE o1.purchase_num = 1
) sub
GROUP BY 1
ORDER BY min(days_to_second);
"""

CUSTOMER_PURCHASE_FREQUENCY = """
SELECT
    CASE
        WHEN order_count = 1 THEN '1 order'
        WHEN order_count = 2 THEN '2 orders'
        WHEN order_count = 3 THEN '3 orders'
        WHEN order_count <= 5 THEN '4-5 orders'
        ELSE '6+ orders'
    END AS frequency_bucket,
    count(*) AS customers,
    avg(total_spent) AS avg_spent
FROM (
    SELECT
        e.customer_id,
        count(DISTINCT e.order_id) AS order_count,
        COALESCE(sum(p.payment_value), 0) AS total_spent
    FROM mart.fact_order_events e
    LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
    WHERE e.event_type = 'order_created'
    GROUP BY 1
) sub
GROUP BY 1
ORDER BY min(order_count);
"""

CUSTOMER_DORMANCY = """
WITH customer_last AS (
    SELECT customer_id, max(event_date) AS last_order
    FROM mart.fact_order_events
    WHERE event_type = 'order_created'
    GROUP BY 1
),
ref AS (SELECT max(event_date) AS ref_date FROM mart.fact_order_events)
SELECT
    CASE
        WHEN (r.ref_date - cl.last_order) <= 30 THEN 'Active (≤30d)'
        WHEN (r.ref_date - cl.last_order) <= 60 THEN 'Cooling (31-60d)'
        WHEN (r.ref_date - cl.last_order) <= 90 THEN 'Dormant (61-90d)'
        WHEN (r.ref_date - cl.last_order) <= 180 THEN 'At Risk (91-180d)'
        ELSE 'Churned (180d+)'
    END AS status,
    count(*) AS customers
FROM customer_last cl
CROSS JOIN ref r
GROUP BY 1
ORDER BY min(r.ref_date - cl.last_order);
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 27: PRICE ELASTICITY
# ═══════════════════════════════════════════════════════════════════════════════

PRICE_VS_VOLUME_BY_CATEGORY = """
SELECT
    COALESCE(p.product_category_name, 'Unknown') AS category,
    avg(i.price) AS avg_price,
    count(*) AS units_sold,
    sum(i.price) AS total_revenue
FROM mart.fact_order_items i
JOIN mart.dim_product p ON p.product_id = i.product_id
GROUP BY 1
HAVING count(*) >= 20
ORDER BY avg_price;
"""

HIGH_INSTALLMENT_IMPACT = """
SELECT
    CASE WHEN payment_installments <= 1 THEN '1 (no split)'
         WHEN payment_installments <= 3 THEN '2-3'
         WHEN payment_installments <= 6 THEN '4-6'
         WHEN payment_installments <= 10 THEN '7-10'
         ELSE '11+'
    END AS installment_group,
    count(DISTINCT order_id) AS orders,
    avg(payment_value) AS avg_payment,
    sum(payment_value) AS total_value
FROM mart.fact_payments
WHERE payment_status = 'approved'
GROUP BY 1
ORDER BY min(payment_installments);
"""

CATEGORY_PRICE_SENSITIVITY = """
WITH cat_monthly AS (
    SELECT
        COALESCE(p.product_category_name, 'Unknown') AS category,
        date_trunc('month', i.event_date)::date AS month,
        avg(i.price) AS avg_price,
        count(*) AS volume
    FROM mart.fact_order_items i
    JOIN mart.dim_product p ON p.product_id = i.product_id
    GROUP BY 1, 2
)
SELECT
    category,
    corr(avg_price, volume) AS price_volume_corr,
    avg(avg_price) AS mean_price,
    avg(volume) AS mean_volume,
    count(*) AS months
FROM cat_monthly
GROUP BY 1
HAVING count(*) >= 6
ORDER BY price_volume_corr ASC
LIMIT 20;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 28: DELIVERY PREDICTION ACCURACY
# ═══════════════════════════════════════════════════════════════════════════════

DELIVERY_ESTIMATION_SCATTER = """
SELECT
    EXTRACT(EPOCH FROM (e.estimated_delivery_ts - e.purchase_ts)) / 86400 AS estimated_days,
    EXTRACT(EPOCH FROM (d.delivered_customer_ts - e.purchase_ts)) / 86400 AS actual_days
FROM mart.fact_delivery_events d
JOIN mart.fact_order_events e ON e.order_id = d.order_id AND e.event_type = 'order_created'
WHERE d.event_type = 'order_delivered'
  AND d.delivered_customer_ts IS NOT NULL
  AND e.estimated_delivery_ts IS NOT NULL
  AND e.purchase_ts IS NOT NULL
ORDER BY random()
LIMIT 2000;
"""

DELIVERY_ESTIMATION_BY_SELLER_STATE = """
SELECT
    s.seller_state,
    count(*) AS deliveries,
    avg(EXTRACT(EPOCH FROM (d.delivered_customer_ts - e.estimated_delivery_ts)) / 86400) AS avg_diff_days,
    100.0 * sum(CASE WHEN d.delivered_customer_ts <= e.estimated_delivery_ts THEN 1 ELSE 0 END)
        / NULLIF(count(*), 0) AS met_estimate_pct
FROM mart.fact_delivery_events d
JOIN mart.fact_order_events e ON e.order_id = d.order_id AND e.event_type = 'order_created'
JOIN mart.dim_seller s ON s.seller_id = d.seller_id
WHERE d.event_type = 'order_delivered'
  AND d.delivered_customer_ts IS NOT NULL
  AND e.estimated_delivery_ts IS NOT NULL
GROUP BY 1
HAVING count(*) >= 20
ORDER BY avg_diff_days DESC;
"""

DELIVERY_OVER_UNDER_PROMISE = """
SELECT
    CASE
        WHEN diff_days < -7 THEN 'Over-promised 7+ days'
        WHEN diff_days < -3 THEN 'Over-promised 3-7 days'
        WHEN diff_days < 0 THEN 'Over-promised 1-3 days'
        WHEN diff_days = 0 THEN 'Exact'
        WHEN diff_days <= 3 THEN 'Under-promised 1-3 days'
        WHEN diff_days <= 7 THEN 'Under-promised 3-7 days'
        ELSE 'Under-promised 7+ days'
    END AS promise_bucket,
    count(*) AS deliveries
FROM (
    SELECT
        EXTRACT(EPOCH FROM (e.estimated_delivery_ts - d.delivered_customer_ts)) / 86400 AS diff_days
    FROM mart.fact_delivery_events d
    JOIN mart.fact_order_events e ON e.order_id = d.order_id AND e.event_type = 'order_created'
    WHERE d.event_type = 'order_delivered'
      AND d.delivered_customer_ts IS NOT NULL
      AND e.estimated_delivery_ts IS NOT NULL
) sub
GROUP BY 1
ORDER BY min(diff_days);
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 29: REVIEW SENTIMENT PROXY
# ═══════════════════════════════════════════════════════════════════════════════

REVIEW_BY_CATEGORY = """
SELECT
    COALESCE(p.product_category_name, 'Unknown') AS category,
    avg(r.review_score) AS avg_score,
    count(*) AS reviews,
    sum(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) AS low_score_count,
    100.0 * sum(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) / NULLIF(count(*), 0) AS low_score_pct
FROM mart.fact_reviews r
JOIN mart.fact_order_items i ON i.order_id = r.order_id
JOIN mart.dim_product p ON p.product_id = i.product_id
GROUP BY 1
HAVING count(*) >= 20
ORDER BY low_score_pct DESC
LIMIT 20;
"""

REVIEW_LOW_SCORE_CAUSES = """
SELECT
    CASE
        WHEN d.delivery_delay_hours > 48 THEN 'Very Late Delivery (>48h)'
        WHEN d.delivery_delay_hours > 0 THEN 'Late Delivery'
        WHEN i.freight_value > 50 THEN 'High Freight (>R$50)'
        WHEN i.price < 20 THEN 'Low Price Item (<R$20)'
        ELSE 'Other'
    END AS likely_cause,
    count(*) AS low_reviews,
    avg(r.review_score) AS avg_score
FROM mart.fact_reviews r
JOIN mart.fact_delivery_events d ON d.order_id = r.order_id AND d.event_type = 'order_delivered'
JOIN mart.fact_order_items i ON i.order_id = r.order_id
WHERE r.review_score <= 2
GROUP BY 1
ORDER BY low_reviews DESC;
"""

REVIEW_BY_SELLER_PERFORMANCE = """
SELECT
    CASE
        WHEN on_time_pct >= 95 THEN 'Excellent (≥95%)'
        WHEN on_time_pct >= 80 THEN 'Good (80-95%)'
        WHEN on_time_pct >= 60 THEN 'Average (60-80%)'
        ELSE 'Poor (<60%)'
    END AS delivery_tier,
    avg(avg_review) AS avg_review_score,
    count(*) AS sellers
FROM (
    SELECT
        d.seller_id,
        100.0 * sum(CASE WHEN d.delivery_delay_hours <= 0 THEN 1 ELSE 0 END)
            / NULLIF(count(*), 0) AS on_time_pct,
        avg(r.review_score) AS avg_review
    FROM mart.fact_delivery_events d
    JOIN mart.fact_reviews r ON r.order_id = d.order_id
    WHERE d.event_type = 'order_delivered'
      AND d.delivery_delay_hours IS NOT NULL
    GROUP BY 1
    HAVING count(*) >= 10
) sub
GROUP BY 1
ORDER BY avg_review_score DESC;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 30: REVENUE DECOMPOSITION
# ═══════════════════════════════════════════════════════════════════════════════

REVENUE_GROWTH_DRIVERS = """
WITH monthly AS (
    SELECT
        date_trunc('month', e.event_date)::date AS month,
        count(DISTINCT e.customer_id) AS customers,
        count(DISTINCT e.order_id) AS orders,
        COALESCE(sum(p.payment_value), 0) AS revenue
    FROM mart.fact_order_events e
    LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
    WHERE e.event_type = 'order_created'
    GROUP BY 1
)
SELECT
    month,
    customers,
    orders,
    revenue,
    CASE WHEN customers > 0 THEN 1.0 * orders / customers ELSE 0 END AS orders_per_customer,
    CASE WHEN orders > 0 THEN revenue / orders ELSE 0 END AS avg_order_value,
    lag(revenue) OVER (ORDER BY month) AS prev_revenue,
    revenue - COALESCE(lag(revenue) OVER (ORDER BY month), 0) AS revenue_change
FROM monthly
ORDER BY month;
"""

REVENUE_NEW_VS_REPEAT = """
WITH first_purchase AS (
    SELECT customer_id, min(event_date) AS first_date
    FROM mart.fact_order_events WHERE event_type = 'order_created'
    GROUP BY 1
)
SELECT
    date_trunc('month', e.event_date)::date AS month,
    CASE WHEN e.event_date = fp.first_date THEN 'New' ELSE 'Repeat' END AS customer_type,
    count(DISTINCT e.order_id) AS orders,
    COALESCE(sum(p.payment_value), 0) AS revenue
FROM mart.fact_order_events e
JOIN first_purchase fp ON fp.customer_id = e.customer_id
LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
WHERE e.event_type = 'order_created'
GROUP BY 1, 2
ORDER BY 1, 2;
"""

REVENUE_WATERFALL = """
WITH monthly AS (
    SELECT
        date_trunc('month', event_date)::date AS month,
        sum(payment_value) AS revenue
    FROM mart.fact_payments
    WHERE payment_status = 'approved'
    GROUP BY 1
    ORDER BY 1
)
SELECT
    month,
    revenue,
    lag(revenue) OVER (ORDER BY month) AS prev_revenue,
    revenue - COALESCE(lag(revenue) OVER (ORDER BY month), 0) AS change
FROM monthly
ORDER BY month;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 31: STATISTICAL SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════

STATS_PRICE = """
SELECT
    'Price' AS metric,
    count(*) AS n,
    avg(price) AS mean,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median,
    stddev(price) AS stddev,
    min(price) AS min_val,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY price) AS q1,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY price) AS q3,
    max(price) AS max_val
FROM mart.fact_order_items;
"""

STATS_FREIGHT = """
SELECT
    'Freight' AS metric,
    count(*) AS n,
    avg(freight_value) AS mean,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY freight_value) AS median,
    stddev(freight_value) AS stddev,
    min(freight_value) AS min_val,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY freight_value) AS q1,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY freight_value) AS q3,
    max(freight_value) AS max_val
FROM mart.fact_order_items;
"""

STATS_DELIVERY_DELAY = """
SELECT
    'Delivery Delay (h)' AS metric,
    count(*) AS n,
    avg(delivery_delay_hours) AS mean,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY delivery_delay_hours) AS median,
    stddev(delivery_delay_hours) AS stddev,
    min(delivery_delay_hours) AS min_val,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY delivery_delay_hours) AS q1,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY delivery_delay_hours) AS q3,
    max(delivery_delay_hours) AS max_val
FROM mart.fact_delivery_events
WHERE event_type = 'order_delivered' AND delivery_delay_hours IS NOT NULL;
"""

STATS_REVIEW = """
SELECT
    'Review Score' AS metric,
    count(*) AS n,
    avg(review_score) AS mean,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY review_score) AS median,
    stddev(review_score) AS stddev,
    min(review_score) AS min_val,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY review_score) AS q1,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY review_score) AS q3,
    max(review_score) AS max_val
FROM mart.fact_reviews;
"""

STATS_PAYMENT_VALUE = """
SELECT
    'Payment Value' AS metric,
    count(*) AS n,
    avg(payment_value) AS mean,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY payment_value) AS median,
    stddev(payment_value) AS stddev,
    min(payment_value) AS min_val,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY payment_value) AS q1,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY payment_value) AS q3,
    max(payment_value) AS max_val
FROM mart.fact_payments
WHERE payment_status = 'approved';
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 32: WHAT-IF SIMULATOR
# ═══════════════════════════════════════════════════════════════════════════════

WHATIF_BASELINE = """
SELECT
    count(DISTINCT e.order_id) AS total_orders,
    COALESCE(sum(p.payment_value), 0) AS total_revenue,
    avg(p.payment_value) AS avg_order_value,
    (SELECT avg(review_score) FROM mart.fact_reviews) AS avg_review,
    (SELECT 100.0 * sum(CASE WHEN delivery_delay_hours <= 0 THEN 1 ELSE 0 END)
        / NULLIF(count(*), 0)
     FROM mart.fact_delivery_events
     WHERE event_type = 'order_delivered' AND delivery_delay_hours IS NOT NULL
    ) AS on_time_pct,
    (SELECT avg(freight_value) FROM mart.fact_order_items) AS avg_freight,
    (SELECT 100.0 * count(DISTINCT CASE WHEN oc > 1 THEN customer_id END)
        / NULLIF(count(DISTINCT customer_id), 0)
     FROM (SELECT customer_id, count(DISTINCT order_id) AS oc
           FROM mart.fact_order_events WHERE event_type='order_created' GROUP BY 1) x
    ) AS repeat_rate_pct
FROM mart.fact_order_events e
LEFT JOIN mart.fact_payments p ON p.order_id = e.order_id AND p.payment_status = 'approved'
WHERE e.event_type = 'order_created';
"""

WHATIF_LATE_DELIVERY_REVENUE_LOSS = """
SELECT
    count(DISTINCT r.order_id) AS late_low_review_orders,
    COALESCE(sum(p.payment_value), 0) AS revenue_at_risk
FROM mart.fact_reviews r
JOIN mart.fact_delivery_events d ON d.order_id = r.order_id AND d.event_type = 'order_delivered'
JOIN mart.fact_payments p ON p.order_id = r.order_id AND p.payment_status = 'approved'
WHERE r.review_score <= 2
  AND d.delivery_delay_hours > 0;
"""

