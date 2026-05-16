"""
dim_sideloader.py  –  One-shot reference-data loader

Builds the `customer_details` and `product_details` reference datasets
(per data-contracts/ingested-data.md §13) and pushes them directly to MinIO.

Not an event stream — does NOT touch Kafka. Run once (or re-run; it is
idempotent and deterministic).

Usage
-----
    python dim_sideloader.py [--config path/to/config.yaml] [--dry-run]
    python dim_sideloader.py --config ../config.yaml --only customers
    python dim_sideloader.py --config ../config.yaml --only products
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("dim_sideloader")


# ── Deterministic Faker keying ───────────────────────────────────────────────
def _stable_seed(key: str) -> int:
    """Stable 64-bit int seed derived from a string key (independent of PYTHONHASHSEED)."""
    h = hashlib.sha1(key.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big", signed=False)


# ── Builders ─────────────────────────────────────────────────────────────────
def build_customer_details(data_dir: Path, locale: str, global_seed: int,
                           contract_version: str, generated_ts: str) -> pd.DataFrame:
    from faker import Faker

    fp = data_dir / "olist_customers_dataset.csv"
    logger.info("Reading %s", fp)
    df = pd.read_csv(fp, dtype=str)

    # Drop Olist's Brazilian geo (per contract §13.3.1).
    df = df[["customer_id", "customer_unique_id"]].copy()

    # Deterministic per-unique-customer attributes.
    Faker.seed(global_seed)
    fake = Faker(locale)

    unique_ids = df["customer_unique_id"].drop_duplicates().tolist()
    logger.info("Generating Faker state/city for %d unique customers …", len(unique_ids))

    mapping: dict[str, tuple[str, str]] = {}
    for uid in unique_ids:
        fake.seed_instance(_stable_seed(uid))
        mapping[uid] = (fake.state_abbr(), fake.city())

    df["state"] = df["customer_unique_id"].map(lambda u: mapping[u][0])
    df["city"] = df["customer_unique_id"].map(lambda u: mapping[u][1])
    df["_generated_ts"] = generated_ts
    df["_contract_version"] = contract_version

    df = df[["customer_id", "customer_unique_id", "state", "city",
             "_generated_ts", "_contract_version"]]
    logger.info("customer_details built: %d rows", len(df))
    return df


def build_product_details(data_dir: Path, locale: str, global_seed: int,
                          contract_version: str, generated_ts: str) -> pd.DataFrame:
    from faker import Faker

    products_fp = data_dir / "olist_products_dataset.csv"
    trans_fp = data_dir / "product_category_name_translation.csv"
    logger.info("Reading %s", products_fp)
    products = pd.read_csv(products_fp, dtype=str)
    logger.info("Reading %s", trans_fp)
    translation = pd.read_csv(trans_fp, dtype=str)

    df = products[["product_id", "product_category_name"]].merge(
        translation, on="product_category_name", how="left"
    )
    df = df.rename(columns={"product_category_name_english": "product_category"})
    df = df[["product_id", "product_category"]].copy()
    df["product_category"] = df["product_category"].where(df["product_category"].notna(), None)

    missing = df["product_category"].isna().sum()
    if missing:
        logger.warning("product_details: %d products have no category mapping", missing)

    Faker.seed(global_seed)
    fake = Faker(locale)
    brands: list[str] = []
    for pid in df["product_id"].tolist():
        fake.seed_instance(_stable_seed(pid))
        brands.append(fake.company())
    df["brand"] = brands

    df["_generated_ts"] = generated_ts
    df["_contract_version"] = contract_version
    df = df[["product_id", "product_category", "brand",
             "_generated_ts", "_contract_version"]]
    logger.info("product_details built: %d rows", len(df))
    return df


# ── Serialisers ──────────────────────────────────────────────────────────────
def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    return buf.getvalue()


def _to_jsonl_bytes(df: pd.DataFrame) -> bytes:
    # orient=records + lines=True → one JSON object per line.
    s = df.to_json(orient="records", lines=True, date_format="iso", force_ascii=False)
    return s.encode("utf-8")


# ── MinIO writer ─────────────────────────────────────────────────────────────
class _S3Writer:
    def __init__(self, cfg: dict):
        import boto3
        from botocore.client import Config

        self.bucket = cfg["reference_bucket"]
        self.prefix = cfg.get("reference_prefix", "reference").strip("/")
        self.s3 = boto3.client(
            "s3",
            endpoint_url=cfg["endpoint_url"],
            aws_access_key_id=cfg["access_key"],
            aws_secret_access_key=cfg["secret_key"],
            region_name=cfg.get("region", "us-east-1"),
            config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
        )
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except Exception:
            logger.info("Bucket %s missing — creating", self.bucket)
            self.s3.create_bucket(Bucket=self.bucket)
        logger.info("S3 writer ready → %s (bucket=%s, prefix=%s)",
                    cfg["endpoint_url"], self.bucket, self.prefix)

    def put(self, key: str, body: bytes, content_type: str) -> str:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type)
        uri = f"s3://{self.bucket}/{key}"
        logger.info("PUT %s (%d bytes)", uri, len(body))
        return uri

    def put_dataset(self, dataset_name: str, df: pd.DataFrame, dt: str) -> list[str]:
        parquet_body = _to_parquet_bytes(df)
        jsonl_body = _to_jsonl_bytes(df)
        keys = [
            (f"{self.prefix}/{dataset_name}/dt={dt}/{dataset_name}.parquet",
             parquet_body, "application/x-parquet"),
            (f"{self.prefix}/{dataset_name}/dt={dt}/{dataset_name}.jsonl",
             jsonl_body, "application/x-ndjson"),
            (f"{self.prefix}/{dataset_name}/_latest/{dataset_name}.parquet",
             parquet_body, "application/x-parquet"),
            (f"{self.prefix}/{dataset_name}/_latest/{dataset_name}.jsonl",
             jsonl_body, "application/x-ndjson"),
        ]
        return [self.put(k, b, ct) for k, b, ct in keys]


class _DryRunWriter:
    def put_dataset(self, dataset_name: str, df: pd.DataFrame, dt: str) -> list[str]:
        logger.info("[DRY-RUN] %s — %d rows, sample:", dataset_name, len(df))
        sample = df.head(3).to_dict(orient="records")
        for r in sample:
            print(json.dumps(r, ensure_ascii=False))
        return []


# ── Orchestration ────────────────────────────────────────────────────────────
def run(cfg: dict, dry_run: bool = False, only: str | None = None) -> None:
    sl_cfg = cfg["sideloader"]
    data_dir = Path(sl_cfg["data_dir"]).resolve()
    locale = sl_cfg.get("faker_locale", "en_US")
    global_seed = int(sl_cfg.get("faker_seed", 42))
    contract_version = str(sl_cfg.get("contract_version", "1.0"))

    if not data_dir.exists():
        raise SystemExit(f"data_dir does not exist: {data_dir}")

    now = datetime.now(timezone.utc)
    generated_ts = now.isoformat()
    dt = now.strftime("%Y-%m-%d")

    writer = _DryRunWriter() if dry_run else _S3Writer(cfg["minio"])

    targets = {"customers", "products"} if only is None else {only}
    if not targets.issubset({"customers", "products"}):
        raise SystemExit(f"--only must be 'customers' or 'products', got {only!r}")

    written: list[str] = []

    if "customers" in targets:
        df = build_customer_details(data_dir, locale, global_seed, contract_version, generated_ts)
        written += writer.put_dataset("customer_details", df, dt)

    if "products" in targets:
        df = build_product_details(data_dir, locale, global_seed, contract_version, generated_ts)
        written += writer.put_dataset("product_details", df, dt)

    if dry_run:
        logger.info("Dry run complete.")
    else:
        logger.info("Sideload complete. %d objects written:", len(written))
        for uri in written:
            logger.info("  %s", uri)


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Olist reference-data sideloader → MinIO")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent.parent / "config.yaml"),
        help="Path to config.yaml",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build datasets and print samples; do not write to MinIO.",
    )
    parser.add_argument(
        "--only",
        choices=["customers", "products"],
        default=None,
        help="Only build/upload one of the two datasets.",
    )
    args = parser.parse_args()

    cfg_path = Path(args.config)
    if not cfg_path.exists():
        logger.error("Config not found: %s", cfg_path)
        sys.exit(1)

    with open(cfg_path) as f:
        cfg = yaml.safe_load(f)

    run(cfg, dry_run=args.dry_run, only=args.only)
