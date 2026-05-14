"""
dumper.py – Kafka → MinIO Raw Dumper (Person 2)
================================================
Consumes all Olist topics, validates each event against the envelope contract,
enriches with Kafka metadata, and writes immutable JSON-Lines files to MinIO:

    s3://datalake/raw/topic=<t>/dt=YYYY-MM-DD/hour=HH/part-<part>-<o1>-<o2>.json

Bad records are routed to the dead-letter bucket. Offsets are committed only
after a successful MinIO upload (at-least-once delivery).

Usage
-----
    python dumper.py [--config ../config.yaml]
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml

sys.path.insert(0, str(Path(__file__).parent))

from kafka_consumer import OlistKafkaConsumer
from minio_writer import MinioRawWriter
from validator import validate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("dumper")

_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    logger.info("Signal %s received – shutting down after current batch…", sig)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _enrich(parsed: Dict[str, Any], msg, contract_version: str) -> Dict[str, Any]:
    """Append Kafka + ingestion metadata per contract §7.3."""
    enriched = dict(parsed)
    key_bytes = msg.key()
    enriched["_kafka_topic"] = msg.topic()
    enriched["_kafka_partition"] = msg.partition()
    enriched["_kafka_offset"] = msg.offset()
    enriched["_kafka_key"] = key_bytes.decode("utf-8", errors="replace") if key_bytes else None
    enriched["_kafka_ingest_ts"] = _now_iso()
    enriched["_raw_file_ts"] = _now_iso()
    enriched["_contract_version"] = contract_version
    return enriched


def _build_dead_letter_record(
    raw_or_text, msg, reason: str, contract_version: str
) -> Dict[str, Any]:
    key_bytes = msg.key()
    return {
        "error_reason": reason,
        "original_payload": raw_or_text,
        "_kafka_topic": msg.topic(),
        "_kafka_partition": msg.partition(),
        "_kafka_offset": msg.offset(),
        "_kafka_key": key_bytes.decode("utf-8", errors="replace") if key_bytes else None,
        "_kafka_ingest_ts": _now_iso(),
        "_raw_file_ts": _now_iso(),
        "_contract_version": contract_version,
    }


def _flush(
    writer: MinioRawWriter,
    good: Dict[tuple, List[Dict[str, Any]]],
    bad: Dict[tuple, List[Dict[str, Any]]],
) -> None:
    for (topic, partition), recs in good.items():
        recs.sort(key=lambda r: r["_kafka_offset"])
        writer.write_raw_batch(topic, partition, recs)
    for (topic, partition), recs in bad.items():
        recs.sort(key=lambda r: r["_kafka_offset"])
        writer.write_dead_letter_batch(topic, partition, recs)


def run(cfg: dict) -> None:
    kcfg = cfg["kafka"]
    mcfg = cfg["minio"]
    dcfg = cfg["dumper"]

    consumer = OlistKafkaConsumer(
        bootstrap_servers=kcfg["bootstrap_servers"],
        group_id=kcfg["group_id"],
        topics=kcfg["topics"],
        auto_offset_reset=kcfg.get("auto_offset_reset", "earliest"),
        enable_auto_commit=kcfg.get("enable_auto_commit", False),
    )
    writer = MinioRawWriter(
        endpoint_url=mcfg["endpoint_url"],
        access_key=mcfg["access_key"],
        secret_key=mcfg["secret_key"],
        raw_bucket=mcfg["raw_bucket"],
        dead_letter_bucket=mcfg["dead_letter_bucket"],
        region=mcfg.get("region", "us-east-1"),
    )

    batch_size = int(dcfg.get("batch_size", 500))
    flush_interval = float(dcfg.get("flush_interval_sec", 30))
    contract_version = str(dcfg.get("contract_version", "1.0"))
    log_every = int(dcfg.get("log_every_n_batches", 5))

    total_good = 0
    total_bad = 0
    batches = 0

    logger.info(
        "Dumper started: batch_size=%d flush_interval=%.1fs contract=%s",
        batch_size, flush_interval, contract_version,
    )

    try:
        while not _shutdown:
            messages = consumer.poll_batch(batch_size, flush_interval)
            if not messages:
                continue

            good: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
            bad: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)

            for msg in messages:
                ok, parsed_or_raw, reason = validate(msg.value())
                tp = (msg.topic(), msg.partition())
                if ok:
                    good[tp].append(_enrich(parsed_or_raw, msg, contract_version))
                else:
                    bad[tp].append(
                        _build_dead_letter_record(
                            parsed_or_raw, msg, reason, contract_version
                        )
                    )

            try:
                _flush(writer, good, bad)
            except Exception:
                logger.exception("Flush to MinIO failed – NOT committing offsets")
                # Skip commit so messages will be redelivered. Short backoff.
                time.sleep(2.0)
                continue

            consumer.commit(messages)

            good_count = sum(len(v) for v in good.values())
            bad_count = sum(len(v) for v in bad.values())
            total_good += good_count
            total_bad += bad_count
            batches += 1

            if batches % log_every == 0:
                logger.info(
                    "batches=%d good=%d bad=%d (last batch: good=%d bad=%d)",
                    batches, total_good, total_bad, good_count, bad_count,
                )

    finally:
        logger.info(
            "Shutting down. batches=%d good=%d bad=%d",
            batches, total_good, total_bad,
        )
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka → MinIO raw dumper")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent.parent / "config.yaml"),
        help="Path to config.yaml (default: ../config.yaml)",
    )
    args = parser.parse_args()

    with open(args.config) as fh:
        cfg = yaml.safe_load(fh)

    run(cfg)
