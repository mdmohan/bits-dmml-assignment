# DMML Assignment work
Build a hybrid ELT pipeline where Olist-based simulated commerce events stream through Kafka, land in MinIO raw lake, are batch-processed by PySpark, and are served from PostgreSQL curated marts for BI and observability.

- **Source data**
- Use Olist dataset as historical truth for realistic entities (`orders`, `items`, `payments`, `customers`, `products`, `sellers`).

- **Event simulation (ingestion producer)**
- Replay/generate timestamp-aligned ecommerce events every second.
- Event types can include `order_created`, `payment_approved`, `order_delivered`, `review_posted`, `inventory_updated`.
- **Tech:** Python, `pandas`, `Faker` (optional), Kafka producer.

- **Streaming ingestion bus**
- Publish all events to Kafka topics with event key and schema version.
- **Tech:** Apache Kafka.

- **Raw landing/dumper**
- Consumer reads Kafka events and dumps immutable JSON/Parquet files into MinIO.
- Raw data is partitioned by topic/date/hour for incremental ELT.
- **Tech:** Kafka consumer, MinIO (S3-compatible object storage).

- **Raw data layout**
- Example path: `s3://datalake/raw/topic=<name>/dt=YYYY-MM-DD/hour=HH/part-*.json`
- Keep metadata fields: `event_id`, `event_ts`, `ingestion_ts`, `schema_version`, `source_app`.

- **Batch ELT transform**
- PySpark reads raw partitions on schedule, validates schema, deduplicates, standardizes fields, derives business columns.
- **Tech:** PySpark, Spark SQL.

- **Processed/curated storage**
- Write cleaned and transformed datasets into PostgreSQL.
- Keep both staging and mart layers.
- **Tech:** PostgreSQL JDBC sink from Spark.

- **Curated table design**
- `staging_events_clean`
- `mart_daily_sales`
- `mart_category_performance`
- `mart_payment_mix`
- `mart_delivery_sla`
- `mart_error_latency` (if app logs included)

- **Data quality controls**
- Check missing critical fields, duplicate event IDs, invalid amounts, timestamp anomalies, schema drift.
- Store quality metrics per run.
- **Tech:** Spark checks (or Great Expectations optional).

- **Orchestration**
- Schedule and monitor replay/batch jobs, with retries and failure alerts.
- **Tech:** Airflow/Astro.

- **Observability**
- Track ingestion lag, rows in/out, bad-row count, job duration, freshness SLA, and alert thresholds.
- **Tech:** Airflow logs + SQL checks + dashboard alerts.

- **BI serving**
- Dashboards read curated PostgreSQL marts for trend and KPI analysis.
- **Tech:** Superset/Metabase/Power BI/Grafana (any one).

- **End-to-end flow**
- Olist tables -> Simulator -> Kafka -> Raw Dumper -> MinIO Raw Lake -> PySpark ELT -> PostgreSQL Curated -> BI Dashboards + Alerts.

- **Subject coverage (high)**
- `L4` (pipelines), `L5` (data infra), `L7` (ingestion), `L8` (validation), `L9` (analytics engineering), `L10` (orchestration), `L13` (distributed processing), `L16` (observability), plus `L2/L3` fundamentals.

