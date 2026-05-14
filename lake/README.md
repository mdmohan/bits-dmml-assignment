# Raw Lake — Kafka → MinIO Dumper

Implements **Person 2** in the data contract: consumes all Olist Kafka topics,
validates each event against the envelope schema, enriches it with Kafka
metadata, and writes immutable JSON-Lines files to MinIO under:

```
s3://datalake/raw/topic=<topic>/dt=YYYY-MM-DD/hour=HH/part-<partition>-<o1>-<o2>.json
```

Bad records are routed to `s3://datalake-dead-letter/raw_dead_letter/...`.
Offsets are committed **only after** a successful MinIO upload (at-least-once).

## Layout
```
lake/
├── config.yaml                  # Host-mode config (localhost:9094, localhost:9001)
├── config.docker.yaml           # Container-mode config (kafka:9092, minio:9000)
├── requirements.txt
├── Dockerfile
├── docker-compose.dumper.yml    # Optional: run as a service on olist-net
├── consumer/
│   ├── dumper.py                # Main loop (entrypoint)
│   ├── kafka_consumer.py        # Consumer wrapper + manual commit
│   ├── minio_writer.py          # boto3 → MinIO (JSON Lines)
│   └── validator.py             # Envelope validation
└── scripts/
    └── validate_minio.py        # Post-run validation utility
```

## Prereqs
The ingest stack must be running (Kafka + MinIO + buckets):

```bash
cd ../ingest
docker compose up -d
```

Verify topics and buckets exist (see [docker-compose.yml](../ingest/docker-compose.yml)).

## Run locally (against host-exposed ports)
```bash
cd lake
pip install -r requirements.txt
python consumer/dumper.py --config config.yaml
```

## Run in Docker
```bash
cd lake
docker compose -f docker-compose.dumper.yml up --build
```

## Validate the raw lake
```bash
python scripts/validate_minio.py --config config.yaml
```

Or use MinIO Web UI at `http://<host>:9002` (login: `minioadmin` / `minioadmin123`).

## Configuration knobs
| Key | Default | Meaning |
|-----|---------|---------|
| `dumper.batch_size` | 500 | Max records per MinIO file |
| `dumper.flush_interval_sec` | 30 | Max time before flushing partial batch |
| `dumper.contract_version` | `1.0` | Embedded in each raw record |
| `kafka.auto_offset_reset` | `earliest` | First-run behaviour |
| `kafka.enable_auto_commit` | `false` | Manual commit only |

## Contract refs
- Envelope: [data-contracts/ingested-data.md §4](../data-contracts/ingested-data.md)
- Raw landing: [data-contracts/ingested-data.md §7](../data-contracts/ingested-data.md)
- Dead-letter: [data-contracts/ingested-data.md §8](../data-contracts/ingested-data.md)
