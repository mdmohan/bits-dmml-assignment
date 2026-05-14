"""
validate_minio.py
Quick health-check for the raw lake. Run after starting the dumper:

    python scripts/validate_minio.py [--config ../config.yaml]

Reports:
  • bucket presence
  • object count per topic
  • sample object preview
  • Kafka metadata field presence on a sample record
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import boto3
import yaml
from botocore.client import Config

REQUIRED_META = (
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_ingest_ts",
    "_contract_version",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent.parent / "config.yaml"),
    )
    args = parser.parse_args()

    with open(args.config) as fh:
        cfg = yaml.safe_load(fh)
    mcfg = cfg["minio"]

    s3 = boto3.client(
        "s3",
        endpoint_url=mcfg["endpoint_url"],
        aws_access_key_id=mcfg["access_key"],
        aws_secret_access_key=mcfg["secret_key"],
        region_name=mcfg.get("region", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )

    print(f"→ Endpoint: {mcfg['endpoint_url']}")
    print(f"→ Raw bucket: {mcfg['raw_bucket']}")
    print(f"→ Dead-letter bucket: {mcfg['dead_letter_bucket']}\n")

    # 1) Bucket presence
    for b in (mcfg["raw_bucket"], mcfg["dead_letter_bucket"]):
        try:
            s3.head_bucket(Bucket=b)
            print(f"[OK]   bucket present: {b}")
        except Exception as exc:
            print(f"[FAIL] bucket missing: {b} ({exc})")
            return 1

    # 2) Object inventory by topic
    print("\n--- Raw object inventory ---")
    paginator = s3.get_paginator("list_objects_v2")
    counts: Counter = Counter()
    total_bytes = 0
    sample_key = None
    for page in paginator.paginate(Bucket=mcfg["raw_bucket"], Prefix="raw/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            total_bytes += obj["Size"]
            # raw/topic=<t>/dt=...
            parts = key.split("/")
            topic = next((p.split("=", 1)[1] for p in parts if p.startswith("topic=")), "?")
            counts[topic] += 1
            sample_key = sample_key or key

    if not counts:
        print("[WARN] no objects under raw/ — dumper may not have flushed yet.")
        return 2

    for topic, n in sorted(counts.items()):
        print(f"  {topic:35s} {n:>5d} files")
    print(f"  {'TOTAL':35s} {sum(counts.values()):>5d} files / {total_bytes:,} bytes")

    # 3) Sample preview
    print(f"\n--- Sample object: {sample_key} ---")
    body = s3.get_object(Bucket=mcfg["raw_bucket"], Key=sample_key)["Body"].read()
    lines = body.decode("utf-8").splitlines()
    print(f"  lines: {len(lines)}")
    if not lines:
        print("[FAIL] empty file")
        return 1

    try:
        first = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        print(f"[FAIL] first line not valid JSON: {exc}")
        return 1

    print("  first record keys:", sorted(first.keys()))
    missing = [k for k in REQUIRED_META if k not in first]
    if missing:
        print(f"[FAIL] missing Kafka metadata: {missing}")
        return 1
    print("[OK]   Kafka metadata fields present.")

    # 4) Dead-letter summary
    print("\n--- Dead-letter inventory ---")
    dl = 0
    for page in paginator.paginate(Bucket=mcfg["dead_letter_bucket"], Prefix="raw_dead_letter/"):
        dl += len(page.get("Contents", []))
    print(f"  dead-letter files: {dl}")

    print("\n✔ Validation complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
