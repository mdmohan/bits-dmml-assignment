"""
minio_writer.py
S3-compatible writer for MinIO. Writes JSON Lines files following the raw-lake
path convention defined in data-contracts/ingested-data.md §7.
"""
from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)


class MinioRawWriter:
    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        raw_bucket: str,
        dead_letter_bucket: str,
        region: str = "us-east-1",
    ):
        self._raw_bucket = raw_bucket
        self._dead_letter_bucket = dead_letter_bucket
        self._s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
        )
        # Best-effort: ensure buckets exist (idempotent).
        for b in (raw_bucket, dead_letter_bucket):
            try:
                self._s3.head_bucket(Bucket=b)
            except Exception:
                try:
                    self._s3.create_bucket(Bucket=b)
                    logger.info("Created bucket: %s", b)
                except Exception as exc:
                    logger.warning("Bucket %s not accessible / not creatable: %s", b, exc)

        logger.info("MinIO writer ready → %s (bucket=%s)", endpoint_url, raw_bucket)

    # ── Path builders ────────────────────────────────────────────────────────
    @staticmethod
    def _partition_path(event_ts_iso: str) -> tuple[str, str]:
        """Derive dt=YYYY-MM-DD/hour=HH from an ISO-8601 event_ts."""
        try:
            ts = datetime.fromisoformat(event_ts_iso.replace("Z", "+00:00"))
            ts = ts.astimezone(timezone.utc)
        except Exception:
            ts = datetime.now(timezone.utc)
        return ts.strftime("%Y-%m-%d"), ts.strftime("%H")

    @staticmethod
    def _object_key(
        prefix: str,
        topic: str,
        dt: str,
        hour: str,
        partition: int,
        offset_start: int,
        offset_end: int,
    ) -> str:
        return (
            f"{prefix}/topic={topic}/dt={dt}/hour={hour}/"
            f"part-{partition}-{offset_start}-{offset_end}.json"
        )

    # ── Writers ──────────────────────────────────────────────────────────────
    def write_raw_batch(
        self,
        topic: str,
        partition: int,
        records: List[Dict[str, Any]],
    ) -> str:
        """Upload a batch (already enriched with _kafka_* metadata) as JSON Lines."""
        if not records:
            return ""

        offset_start = records[0]["_kafka_offset"]
        offset_end = records[-1]["_kafka_offset"]
        # Use the earliest event_ts in the batch to pick partition path.
        event_ts = records[0].get("event_ts") or records[0].get("_kafka_ingest_ts")
        dt, hour = self._partition_path(event_ts)

        key = self._object_key("raw", topic, dt, hour, partition, offset_start, offset_end)
        body = self._serialize(records)
        self._put(self._raw_bucket, key, body)
        logger.info(
            "MinIO PUT s3://%s/%s (%d records)",
            self._raw_bucket, key, len(records),
        )
        return key

    def write_dead_letter_batch(
        self,
        topic: str,
        partition: int,
        bad_records: List[Dict[str, Any]],
    ) -> str:
        """bad_records: each has {error_reason, original_payload, _kafka_*}."""
        if not bad_records:
            return ""

        offset_start = bad_records[0]["_kafka_offset"]
        offset_end = bad_records[-1]["_kafka_offset"]
        dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        hour = datetime.now(timezone.utc).strftime("%H")

        key = self._object_key(
            "raw_dead_letter", topic, dt, hour, partition, offset_start, offset_end
        )
        body = self._serialize(bad_records)
        self._put(self._dead_letter_bucket, key, body)
        logger.warning(
            "Dead-letter PUT s3://%s/%s (%d bad records)",
            self._dead_letter_bucket, key, len(bad_records),
        )
        return key

    # ── Internals ────────────────────────────────────────────────────────────
    @staticmethod
    def _serialize(records: Iterable[Dict[str, Any]]) -> bytes:
        buf = io.BytesIO()
        for rec in records:
            buf.write(json.dumps(rec, default=str, ensure_ascii=False).encode("utf-8"))
            buf.write(b"\n")
        return buf.getvalue()

    def _put(self, bucket: str, key: str, body: bytes) -> None:
        self._s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
        )
