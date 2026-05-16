# Olist Event Stream Data Contract (v1)

## 1. Purpose
This contract defines the event schema, delivery guarantees, topic design, and raw lake layout for Olist-based simulated ecommerce events.

Producer: **Person 1 (Simulator + Kafka Producer)**  
Consumer: **Person 2 (Kafka -> MinIO Raw Dumper)**  
Downstream: **Person 3 (Spark ELT)**

## 2. Event Transport
- **Bus:** Kafka
- **Encoding:** JSON (UTF-8)
- **Compression:** optional (`snappy` or `gzip`)
- **Delivery:** at-least-once
- **Ordering:** guaranteed per Kafka partition key
- **Partition key:** `order_id` (or `customer_id` for customer-level events)
- **Timestamp source:** `event_ts` from simulator (Olist timestamp-aligned replay)

## 3. Topics
- `olist.order_events`
- `olist.payment_events`
- `olist.delivery_events`
- `olist.review_events`
- `olist.inventory_events` (optional simulated extension)

Retention recommendation:
- Kafka topic retention: `3-7 days`
- Raw lake retention: long-term (`>=90 days` or project default)

## 4. Canonical Event Envelope (Required for all topics)
```json
{
  "event_id": "uuid-string",
  "event_type": "order_created|order_approved|payment_approved|order_shipped|order_delivered|review_created|inventory_updated",
  "schema_version": "1.0",
  "event_ts": "2026-05-12T10:20:30Z",
  "ingestion_hint_ts": "2026-05-12T10:20:31Z",
  "source_system": "olist_simulator",
  "source_table": "orders|payments|order_reviews|order_items",
  "trace_id": "uuid-string",
  "order_id": "string",
  "customer_id": "string|null",
  "seller_id": "string|null",
  "payload": {}
}
```

Required fields:
- `event_id` (unique)
- `event_type`
- `schema_version`
- `event_ts` (ISO-8601 UTC)
- `source_system`
- `order_id`
- `payload` (object)

## 5. Topic-wise Payload Contracts

### 5.1 `olist.order_events`
`event_type` examples:
- `order_created`
- `order_approved`
- `order_canceled`

Payload:
```json
{
  "order_status": "created|approved|canceled|processing",
  "purchase_ts": "timestamp|null",
  "approved_ts": "timestamp|null",
  "estimated_delivery_ts": "timestamp|null",
  "order_value": 1234.56,
  "item_count": 3
}
```

### 5.2 `olist.payment_events`
`event_type`:
- `payment_approved`
- `payment_failed` (simulated optional)

Payload:
```json
{
  "payment_sequential": 1,
  "payment_type": "credit_card|boleto|voucher|debit_card",
  "payment_installments": 3,
  "payment_value": 456.78,
  "payment_status": "approved|failed"
}
```

### 5.3 `olist.delivery_events`
`event_type`:
- `order_shipped`
- `order_delivered`

Payload:
```json
{
  "carrier_status": "shipped|in_transit|delivered",
  "shipped_ts": "timestamp|null",
  "delivered_customer_ts": "timestamp|null",
  "delivery_delay_hours": 12.5
}
```

### 5.4 `olist.review_events`
`event_type`:
- `review_created`

Payload:
```json
{
  "review_id": "string",
  "review_score": 1,
  "review_created_ts": "timestamp",
  "review_answer_ts": "timestamp|null",
  "review_comment_title": "string|null"
}
```

### 5.5 `olist.inventory_events` (optional)
`event_type`:
- `inventory_updated`

Payload:
```json
{
  "product_id": "string",
  "sku": "string",
  "delta_qty": -2,
  "stock_after": 98,
  "warehouse": "SP1"
}
```

## 6. Data Types and Constraints
- `event_id`: UUID string, globally unique
- `schema_version`: semantic version (`major.minor`)
- `event_ts`: UTC timestamp in ISO-8601
- `order_id`: non-null string
- `payment_value/order_value`: decimal >= 0
- `review_score`: integer in `[1,5]`
- `event_type`: must match allowed enum for topic

## 7. MinIO Raw Landing Contract (for Person 2)

### 7.1 Path Convention
`s3://<bucket>/raw/topic=<topic_name>/dt=YYYY-MM-DD/hour=HH/part-<kafka_partition>-<offset_start>-<offset_end>.json`

Example:  
`s3://datalake/raw/topic=olist.order_events/dt=2026-05-12/hour=10/part-2-4500-4899.json`

### 7.2 File Format
- JSON Lines (`.json`) preferred for debugging
- Optional parallel write in Parquet for analytics acceleration

### 7.3 Raw Record (must preserve original + ingest metadata)
Each dumped row must include:
- full original Kafka message
- `_kafka_topic`
- `_kafka_partition`
- `_kafka_offset`
- `_kafka_key`
- `_kafka_ingest_ts`
- `_raw_file_ts`
- `_contract_version` = `1.0`

No destructive transforms in raw zone.

## 8. Error Handling Contract
- Invalid JSON -> route to `raw_dead_letter` path
- Missing required envelope fields -> route to `raw_dead_letter`
- Wrong data type -> route to `raw_dead_letter`

Dead-letter path:
- `s3://<bucket>/raw_dead_letter/topic=<topic>/dt=YYYY-MM-DD/hour=HH/...`

Include `error_reason` and original payload in dead-letter record.

## 9. Idempotency + Duplication Rules
- `event_id` is business dedupe key.
- At-least-once Kafka may create duplicates; raw layer stores as-is.
- Dedupe is performed downstream in Spark ELT (Person 3), not in raw dump.

## 10. Schema Evolution Rules
- Backward-compatible additive fields allowed under same major version.
- Breaking changes require major version bump (`2.x`).
- Producer must publish changelog before activation.
- Consumer must preserve unknown fields in raw storage.

## 11. SLA / Operational Expectations
- Event publish interval: configurable (default 1 event/sec per stream in demo)
- Max tolerated producer-consumer lag (demo): `< 60s`
- Raw dumper flush interval: every `N` records or `T` seconds (configurable)
- Availability target for demo pipeline: `>= 95%` successful batches

## 12. Ownership
- Producer schema owner: Person 1
- Raw landing owner: Person 2
- ELT contract consumer: Person 3
- Analytics consumer: Person 4

## 13. Reference / Side-loaded Datasets

### 13.1 Purpose
Slowly-changing dimension data that downstream analytics (Spark ELT, warehouse dims) needs in order to enrich event streams with descriptive attributes. These datasets are **not events** and **not delivered via Kafka**. They are produced by the ingest module and pushed directly to MinIO.

### 13.2 Transport
- **Bus:** none (direct S3/MinIO PUT)
- **Producer:** ingest (`ingest/producer/dim_sideloader.py`)
- **Consumers:** Spark ELT (transform), warehouse dim loaders
- **Refresh cadence:** one-shot for the demo; script is idempotent and safe to re-run
- **Envelope:** none — flat records, not wrapped in the §4 event envelope

### 13.3 Datasets

#### 13.3.1 `customer_details`
| Field                | Type   | Source                                | Notes |
|----------------------|--------|---------------------------------------|-------|
| `customer_id`        | string | Olist `olist_customers_dataset.csv`   | authoritative join key for events |
| `customer_unique_id` | string | Olist                                 | retained for repeat-customer analytics |
| `state`              | string | **Faker** (`en_US`, state abbreviation) | synthesized; deterministic per `customer_unique_id` |
| `city`               | string | **Faker** (`en_US`, `city()`)         | synthesized; deterministic per `customer_unique_id` |
| `_generated_ts`      | string | producer wall clock (ISO-8601 UTC)    | provenance |
| `_contract_version`  | string | `1.0`                                 | |

Constraints:
- `customer_id` unique, non-null.
- Same `customer_unique_id` always maps to the same `state`/`city` (Faker seeded per unique id).
- Olist's original Brazilian `customer_state` / `customer_city` columns are **dropped** — replaced, not augmented, to avoid two conflicting "state" columns downstream.

#### 13.3.2 `product_details`
| Field               | Type   | Source                                              | Notes |
|---------------------|--------|-----------------------------------------------------|-------|
| `product_id`        | string | Olist `olist_products_dataset.csv`                  | authoritative join key for `inventory_events` |
| `product_category`  | string | `product_category_name_translation.csv` (English)   | left-joined on `product_category_name`; null-safe |
| `brand`             | string | **Faker** (`company()`)                             | synthesized; deterministic per `product_id` |
| `_generated_ts`     | string | producer wall clock (ISO-8601 UTC)                  | |
| `_contract_version` | string | `1.0`                                               | |

Constraints:
- `product_id` unique, non-null.
- `product_category` may be null (Olist has products without category).
- Olist's physical attributes (`product_weight_g`, dimensions, etc.) are **not included** in v1.

### 13.4 MinIO Layout
```
s3://datalake/reference/customer_details/dt=YYYY-MM-DD/customer_details.parquet
s3://datalake/reference/customer_details/dt=YYYY-MM-DD/customer_details.jsonl
s3://datalake/reference/customer_details/_latest/customer_details.parquet
s3://datalake/reference/customer_details/_latest/customer_details.jsonl
s3://datalake/reference/product_details/dt=YYYY-MM-DD/product_details.parquet
s3://datalake/reference/product_details/dt=YYYY-MM-DD/product_details.jsonl
s3://datalake/reference/product_details/_latest/product_details.parquet
s3://datalake/reference/product_details/_latest/product_details.jsonl
```

- **Bucket:** reuses the existing `datalake` bucket; prefix `reference/` segregates it from `raw/` (events) and `raw_dead_letter/`.
- **Formats:** Parquet is the canonical read format for Spark; JSON Lines is kept alongside for human inspection.
- **`_latest/` pointer:** stable path consumers read by default; dated partitions are history snapshots.

### 13.5 File Format Details
- Parquet: snappy compression, single file per dataset.
- JSON Lines: one record per line, UTF-8, no envelope wrapper.

### 13.6 Determinism / Reproducibility
- Global Faker seed: fixed constant (`42`) declared in the script.
- Per-row seeding: `Faker.seed_instance(stable_hash(key))` where `key` is `customer_unique_id` or `product_id`, so a given id always produces the same fabricated attributes across re-runs.

### 13.7 Error Handling
- Missing source CSV → fail fast, no partial writes.
- Translation join misses → emit row with `product_category = null` (logged, not dead-lettered).
- No dead-letter path for reference data (single-shot batch; failure means re-run).

### 13.8 Ownership
| Concern                  | Owner   |
|--------------------------|---------|
| Schema + producer        | Person 1 (ingest) |
| Spark read + join        | Person 3 (transform) |
| Warehouse dim load       | Person 4 (warehouse) |
| Path / bucket convention | Person 1 (ingest) |

### 13.9 Out of Scope (v1)
- Periodic sync / CDC of customer or product master.
- A Kafka `reference.*` topic.
- `customer_created` / `product_added` event modeling.
- Enrichment of in-flight event payloads with denormalized fields (the join is performed downstream in Spark).

