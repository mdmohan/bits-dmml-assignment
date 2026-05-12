# Star Schema Design: Olist-Based ELT Analytics Warehouse

## 1) Overview
This document defines the **star schema** for the curated warehouse layer built from Olist-based event streams and batch data.

Goal:
- support BI dashboards
- support fast KPI queries
- preserve clean relationships between events, payments, delivery, reviews, sellers, customers, and products

Design pattern:
- **Fact tables** store measurable events/metrics
- **Dimension tables** store descriptive attributes
- Optional bridge fact (`fact_order_items`) links order-level and product-level analysis cleanly

---

## 2) Fact Tables

## 2.1 `fact_order_events`
- **Purpose:** Track order lifecycle (`created`, `approved`, `canceled`, etc.).
- **Grain:** One row per `order_id + event_type + event_ts`.

Columns:
- `event_id` `TEXT` **PK**
- `order_id` `TEXT` **NOT NULL**
- `event_type` `TEXT` **NOT NULL**
- `event_ts` `TIMESTAMP` **NOT NULL**
- `event_date` `DATE` **NOT NULL**
- `customer_id` `TEXT`
- `seller_id` `TEXT`
- `order_status` `TEXT`
- `purchase_ts` `TIMESTAMP`
- `approved_ts` `TIMESTAMP`
- `estimated_delivery_ts` `TIMESTAMP`
- `source_system` `TEXT` **NOT NULL**
- `schema_version` `TEXT` **NOT NULL**
- `ingestion_ts` `TIMESTAMP` **NOT NULL**

Indexes:
- `(event_date)`
- `(order_id, event_type)`
- `(seller_id, event_date)`

---

## 2.2 `fact_payments`
- **Purpose:** Payment analytics and payment-method KPIs.
- **Grain:** One row per `order_id + payment_sequential + event_ts`.

Columns:
- `event_id` `TEXT` **PK**
- `order_id` `TEXT` **NOT NULL**
- `payment_sequential` `INT` **NOT NULL**
- `payment_type` `TEXT` **NOT NULL**
- `payment_installments` `INT`
- `payment_value` `NUMERIC(18,2)` **NOT NULL**
- `payment_status` `TEXT` **NOT NULL** (`approved`/`failed`)
- `event_ts` `TIMESTAMP` **NOT NULL**
- `event_date` `DATE` **NOT NULL**
- `customer_id` `TEXT`
- `source_system` `TEXT` **NOT NULL**
- `schema_version` `TEXT` **NOT NULL**
- `ingestion_ts` `TIMESTAMP` **NOT NULL**

Indexes:
- `(event_date, payment_type)`
- `(order_id)`
- `(payment_status, event_date)`

---

## 2.3 `fact_delivery_events`
- **Purpose:** Delivery SLA and logistics performance.
- **Grain:** One row per `order_id + delivery_event_type + event_ts`.

Columns:
- `event_id` `TEXT` **PK**
- `order_id` `TEXT` **NOT NULL**
- `event_type` `TEXT` **NOT NULL** (`order_shipped`, `order_delivered`)
- `carrier_status` `TEXT`
- `shipped_ts` `TIMESTAMP`
- `delivered_customer_ts` `TIMESTAMP`
- `delivery_delay_hours` `NUMERIC(10,2)`
- `event_ts` `TIMESTAMP` **NOT NULL**
- `event_date` `DATE` **NOT NULL**
- `seller_id` `TEXT`
- `customer_id` `TEXT`
- `source_system` `TEXT` **NOT NULL**
- `schema_version` `TEXT` **NOT NULL**
- `ingestion_ts` `TIMESTAMP` **NOT NULL**

Indexes:
- `(event_date, seller_id)`
- `(order_id)`
- `(delivery_delay_hours)`

---

## 2.4 `fact_reviews`
- **Purpose:** Review/sentiment quality metrics.
- **Grain:** One row per `review_id` event.

Columns:
- `event_id` `TEXT` **PK**
- `review_id` `TEXT` **NOT NULL**
- `order_id` `TEXT` **NOT NULL**
- `review_score` `INT` **NOT NULL** (`1..5`)
- `review_created_ts` `TIMESTAMP` **NOT NULL**
- `review_answer_ts` `TIMESTAMP`
- `review_response_hours` `NUMERIC(10,2)`
- `review_comment_title` `TEXT`
- `event_ts` `TIMESTAMP` **NOT NULL**
- `event_date` `DATE` **NOT NULL**
- `customer_id` `TEXT`
- `seller_id` `TEXT`
- `source_system` `TEXT` **NOT NULL**
- `schema_version` `TEXT` **NOT NULL**
- `ingestion_ts` `TIMESTAMP` **NOT NULL**

Indexes:
- `(event_date, review_score)`
- `(order_id)`
- `(seller_id, event_date)`

---

## 2.5 `fact_order_items` (Recommended bridge fact)
- **Purpose:** Item-level revenue and join path to product/seller.
- **Grain:** One row per `order_id + order_item_id`.

Columns:
- `order_id` `TEXT` **NOT NULL**
- `order_item_id` `INT` **NOT NULL**
- `product_id` `TEXT` **NOT NULL**
- `seller_id` `TEXT` **NOT NULL**
- `shipping_limit_date` `TIMESTAMP`
- `price` `NUMERIC(18,2)`
- `freight_value` `NUMERIC(18,2)`
- `event_date` `DATE`
- `PRIMARY KEY (order_id, order_item_id)`

Indexes:
- `(product_id)`
- `(seller_id, event_date)`
- `(order_id)`

---

## 2.6 `fact_data_quality` (Operational fact)
- **Purpose:** Track pipeline quality and run health metrics.
- **Grain:** One row per `run_id + dataset_name`.

Columns:
- `run_id` `TEXT` **NOT NULL**
- `dataset_name` `TEXT` **NOT NULL**
- `run_ts` `TIMESTAMP` **NOT NULL**
- `rows_in` `BIGINT`
- `rows_out` `BIGINT`
- `null_violations` `BIGINT`
- `duplicate_violations` `BIGINT`
- `schema_violations` `BIGINT`
- `status` `TEXT` (`success`/`failed`)
- `PRIMARY KEY (run_id, dataset_name)`

---

## 3) Dimension Tables

## 3.1 `dim_date`
- **Purpose:** Calendar intelligence for all time-based reporting.
- **Grain:** One row per date.

Columns:
- `date_key` `INT` **PK** (`YYYYMMDD`)
- `full_date` `DATE` **UNIQUE NOT NULL**
- `day_of_week` `INT`
- `day_name` `TEXT`
- `day_of_month` `INT`
- `week_of_year` `INT`
- `month` `INT`
- `month_name` `TEXT`
- `quarter` `INT`
- `year` `INT`
- `is_weekend` `BOOLEAN`
- `is_month_start` `BOOLEAN`
- `is_month_end` `BOOLEAN`

---

## 3.2 `dim_customer`
- **Purpose:** Customer geography and profile slicing.
- **Grain:** One row per `customer_id`.

Columns:
- `customer_key` `BIGSERIAL` **PK**
- `customer_id` `TEXT` **UNIQUE NOT NULL**
- `customer_unique_id` `TEXT`
- `customer_city` `TEXT`
- `customer_state` `TEXT`
- `customer_zip_prefix` `TEXT`
- `created_at` `TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
- `updated_at` `TIMESTAMP`

Indexes:
- `(customer_state, customer_city)`

---

## 3.3 `dim_seller`
- **Purpose:** Seller-level segmentation and SLA slicing.
- **Grain:** One row per `seller_id`.

Columns:
- `seller_key` `BIGSERIAL` **PK**
- `seller_id` `TEXT` **UNIQUE NOT NULL**
- `seller_city` `TEXT`
- `seller_state` `TEXT`
- `seller_zip_prefix` `TEXT`
- `created_at` `TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
- `updated_at` `TIMESTAMP`

Indexes:
- `(seller_state, seller_city)`

---

## 3.4 `dim_product`
- **Purpose:** Product/category attributes for assortment analytics.
- **Grain:** One row per `product_id`.

Columns:
- `product_key` `BIGSERIAL` **PK**
- `product_id` `TEXT` **UNIQUE NOT NULL**
- `product_category_name` `TEXT`
- `product_name_lenght` `INT`
- `product_description_lenght` `INT`
- `product_photos_qty` `INT`
- `product_weight_g` `INT`
- `product_length_cm` `INT`
- `product_height_cm` `INT`
- `product_width_cm` `INT`
- `created_at` `TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
- `updated_at` `TIMESTAMP`

Indexes:
- `(product_category_name)`

---

## 3.5 `dim_payment_type` (Optional)
- **Purpose:** Normalized payment method dimension.
- **Grain:** One row per payment type.

Columns:
- `payment_type_key` `SMALLSERIAL` **PK**
- `payment_type` `TEXT` **UNIQUE NOT NULL**
- `is_digital` `BOOLEAN`
- `is_installment_supported` `BOOLEAN`

---

## 3.6 `dim_event_type` (Optional)
- **Purpose:** Controlled event vocabulary and governance.
- **Grain:** One row per event type.

Columns:
- `event_type_key` `SMALLSERIAL` **PK**
- `event_type` `TEXT` **UNIQUE NOT NULL**
- `domain` `TEXT` (`order`, `payment`, `delivery`, `review`, `inventory`)
- `is_terminal_state` `BOOLEAN`

---

## 4) Logical Relationships

Core joins:
- `fact_order_events.customer_id` -> `dim_customer.customer_id`
- `fact_order_events.seller_id` -> `dim_seller.seller_id`
- `fact_payments.customer_id` -> `dim_customer.customer_id`
- `fact_delivery_events.seller_id` -> `dim_seller.seller_id`
- `fact_reviews.order_id` -> `fact_order_events.order_id`
- `fact_order_items.product_id` -> `dim_product.product_id`
- `fact_order_items.seller_id` -> `dim_seller.seller_id`
- all event/fact dates -> `dim_date.full_date`

---

## 5) Data Quality and Integrity Rules
- `event_id` unique within each fact table.
- `review_score BETWEEN 1 AND 5`.
- `payment_value >= 0`.
- `order_value >= 0`.
- `event_ts` must be parseable UTC timestamp.
- Dedupe key in ETL: `event_id`.
- Late-arriving events allowed; ingestion watermark tracked separately.

---

## 6) SCD Strategy (Assignment-friendly)
Use **Type 1** for all dimensions:
- overwrite with latest value
- keep `created_at`, `updated_at` for audit trail

Can evolve to Type 2 later if history tracking is required.

---

## 7) Suggested Dashboard-ready Aggregated Marts
(derived from facts + dims)

- `mart_daily_sales` (`date x category x state`)
- `mart_payment_mix` (`date x payment_type`)
- `mart_delivery_sla` (`date x seller`)
- `mart_order_funnel` (`date`)
- `mart_review_quality` (`date x category`)
- `mart_data_quality` (`run_id x dataset`)

---

## 8) Ownership Boundary (Person 3 and Person 4)
- Person 3 owns physical schema, ETL logic, refresh guarantees.
- Person 4 owns BI semantics, dashboard metrics, observability thresholds.
- Any breaking schema change requires version bump + changelog.
```

