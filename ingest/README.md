# Ingest – Olist Event Simulator

Replays the Olist e-commerce dataset as a stream of Kafka events, preserving the original chronological order and timestamp gaps between events. A configurable **scale** factor controls replay speed.

## Architecture

```
data/*.csv → olist_loader.py → event_builder.py → kafka_producer.py → Kafka topics
                 ↓                                        ↑
         build_event_timeline()              (or _ConsoleProducer in --dry-run)
```

**Kafka Topics produced:**

| Topic | Event Types |
|-------|-------------|
| `olist.order_events` | order_created, order_approved |
| `olist.payment_events` | payment_received |
| `olist.delivery_events` | order_shipped, order_delivered |
| `olist.review_events` | review_submitted |
| `olist.inventory_events` | inventory_item_added |

## Data Setup

Place the following Olist CSV files in `ingest/data/`:

```
olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_orders_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

These are from the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) on Kaggle.

## Configuration

### `config.yaml` (local development)

```yaml
kafka:
  bootstrap_servers: "localhost:9094"

simulator:
  data_dir: "../data"
  scale: 3600      # 1 hour of data replays in 1 second
  loop: true
  log_every_n: 200
```

### `config.docker.yaml` (Docker container)

```yaml
kafka:
  bootstrap_servers: "kafka:9092"

simulator:
  data_dir: "../data"
  scale: 3600
  loop: true
  log_every_n: 50
```

### Scale parameter

| scale | Meaning |
|-------|---------|
| 1 | Real-time (original gaps) |
| 60 | 60× faster – 1 hour of data in 1 minute |
| 3600 | 3600× faster – 1 hour of data in 1 second |
| 86400 | 86400× faster – 1 day of data in 1 second |

## Running Locally

### Prerequisites

- Python 3.11+
- Kafka running (e.g. via `docker compose up -d` from this directory)

### Install dependencies

```bash
cd ingest
pip install -r requirements.txt
```

### Run (live – publishes to Kafka)

```bash
cd ingest/producer
python simulator.py --config ../config.yaml
```

### Run (dry-run – prints to console, no Kafka needed)

```bash
cd ingest/producer
python simulator.py --config ../config.yaml --dry-run
```

## Running with Docker

### Start the infrastructure stack (Kafka, Kafdrop, MinIO)

```bash
cd ingest
docker compose up -d
```

Services:
- **Kafka** – `localhost:9094` (external) / `kafka:9092` (internal)
- **Kafdrop** – `http://localhost:9000`
- **MinIO Console** – `http://localhost:9002` (minioadmin / minioadmin123)

### Run the simulator container

**Dry-run (no Kafka needed):**

```bash
docker compose -f docker-compose.simulator.yml up simulator-dry-run
```

**Live (publishes to Kafka):**

```bash
docker compose -f docker-compose.simulator.yml up -d simulator
```

**Rebuild after code changes:**

```bash
docker compose -f docker-compose.simulator.yml up -d --build simulator
```

**View logs:**

```bash
docker compose -f docker-compose.simulator.yml logs -f simulator
```

**Stop:**

```bash
docker compose -f docker-compose.simulator.yml down
```

## Project Structure

```
ingest/
├── config.yaml                  # Local dev config
├── config.docker.yaml           # Docker config
├── docker-compose.yml           # Infra stack (Kafka, Kafdrop, MinIO)
├── docker-compose.simulator.yml # Simulator + dim-sideloader containers
├── Dockerfile                   # Simulator image
├── Dockerfile.sideloader        # Dim sideloader image
├── requirements.txt             # Python deps
├── data/                        # Olist CSV files (not committed)
│   └── *.csv
└── producer/
    ├── simulator.py             # Main entry point – timestamp-driven replay
    ├── olist_loader.py          # Loads CSVs, builds sorted event timeline
    ├── event_builder.py         # Maps timeline rows to Kafka message envelopes
    ├── kafka_producer.py        # Thin confluent-kafka wrapper
    └── dim_sideloader.py        # One-shot reference-data loader → MinIO
```

## Reference Data Sideloader (customer_details / product_details)

Customer and product master data are **not** events. They are pushed
directly to MinIO as static reference datasets — per
`data-contracts/ingested-data.md` §13. Run once for the demo; the
script is idempotent and deterministic.

Output (in the existing `datalake` bucket):

```
s3://datalake/reference/customer_details/dt=YYYY-MM-DD/customer_details.{parquet,jsonl}
s3://datalake/reference/customer_details/_latest/customer_details.{parquet,jsonl}
s3://datalake/reference/product_details/dt=YYYY-MM-DD/product_details.{parquet,jsonl}
s3://datalake/reference/product_details/_latest/product_details.{parquet,jsonl}
```

### Run locally (against MinIO on the host)

```bash
cd ingest
pip install -r requirements.txt
cd producer
python dim_sideloader.py --config ../config.yaml                 # write both
python dim_sideloader.py --config ../config.yaml --dry-run       # print samples
python dim_sideloader.py --config ../config.yaml --only products # one dataset
```

### Run via Docker (one-shot)

Make sure the infra stack (which contains MinIO) is up first:

```bash
cd ingest
docker compose up -d
```

Then run the sideloader as a one-shot container:

```bash
docker compose -f docker-compose.simulator.yml run --rm dim-sideloader
# variants:
docker compose -f docker-compose.simulator.yml run --rm dim-sideloader --dry-run
docker compose -f docker-compose.simulator.yml run --rm dim-sideloader --only customers
```

Rebuild after code changes:

```bash
docker compose -f docker-compose.simulator.yml build dim-sideloader
```

### Verify the upload

```bash
# Via the MinIO console:  http://localhost:9002  (minioadmin / minioadmin123)
# Or via mc:
mc alias set local http://localhost:9001 minioadmin minioadmin123
mc ls --recursive local/datalake/reference/
mc cat local/datalake/reference/customer_details/_latest/customer_details.jsonl | head -3
```