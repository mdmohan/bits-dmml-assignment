from __future__ import annotations

from abc import ABC, abstractmethod
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, input_file_name, coalesce, to_timestamp, current_timestamp, expr

from spark.jdbc import DbWriter
from spark.metadata import (
    ensure_etl_watermark_table,
    get_topic_watermark,
    max_event_ts,
    upsert_topic_watermark,
)


class BaseEventProcessor(ABC):
    topic_key: str = ""

    def __init__(self, spark: SparkSession, db_writer: DbWriter, source_path: str):
        self.spark = spark
        self.db_writer = db_writer
        self.source_path = source_path

    @abstractmethod
    def read_raw(self) -> DataFrame:
        pass

    @abstractmethod
    def build_dimensions(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        pass

    @abstractmethod
    def build_facts(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        pass

    def table_keys(self) -> dict[str, list[str]]:
        return {
            "mart.dim_customer": ["customer_id"],
            "mart.dim_seller": ["seller_id"],
            "mart.fact_order_events": ["event_id"],
            "mart.fact_payments": ["event_id"],
            "mart.fact_delivery_events": ["event_id"],
            "mart.fact_reviews": ["event_id"],
        }

    def _idempotent_new_rows(self, table: str, df: DataFrame) -> DataFrame:
        keys = self.table_keys().get(table, [])
        if not keys:
            return df
        try:
            existing = self.db_writer.read_table(self.spark, table).select(*keys).dropDuplicates(keys)
            return df.join(existing, on=keys, how="left_anti")
        except Exception:
            return df

    def write(self, datasets: dict[str, DataFrame]) -> tuple[int, int]:
        written = 0
        duplicates = 0
        for table, df in datasets.items():
            if df is None:
                continue
            total_before = df.count()
            df_new = self._idempotent_new_rows(table, df)
            total_after = df_new.count()
            duplicates += max(total_before - total_after, 0)
            if df_new.rdd.isEmpty():
                continue
            self.db_writer.write_append_jdbc(df_new, table)
            written += total_after
        return written, duplicates

    def run(self) -> dict[str, int]:
        ensure_etl_watermark_table(self.db_writer)
        last_ts, seen_keys = get_topic_watermark(self.spark, self.db_writer, self.topic_key)

        raw_df = self.read_raw().withColumn("_source_file", input_file_name())
        window_minutes = int(os.getenv("WINDOW_MINUTES", "20"))
        raw_df = raw_df.withColumn(
            "_ingest_ts",
            coalesce(
                to_timestamp(col("_kafka_ingest_ts")),
                to_timestamp(col("_raw_file_ts")),
                to_timestamp(col("ingestion_hint_ts")),
            ),
        ).where(col("_ingest_ts").isNotNull())
        raw_df = raw_df.where(col("_ingest_ts") >= (current_timestamp() - expr(f"INTERVAL {window_minutes} MINUTES")))

        if seen_keys:
            raw_df = raw_df.where(~col("_source_file").isin(list(seen_keys)))

        raw_count = raw_df.count()
        if raw_count == 0:
            return {
                "raw_count": 0,
                "dim_written": 0,
                "fact_written": 0,
                "duplicates_skipped": 0,
                "status": "no_new_data",
            }

        dims = self.build_dimensions(raw_df)
        facts = self.build_facts(raw_df)

        dim_written, dim_dups = self.write(dims)
        fact_written, fact_dups = self.write(facts)

        new_last_ts = max_event_ts(raw_df)
        new_files = [r["_source_file"] for r in raw_df.select("_source_file").dropDuplicates().collect()]
        merged_files = list(seen_keys.union(set(new_files)))
        upsert_topic_watermark(self.db_writer, self.topic_key, new_last_ts, merged_files)

        return {
            "raw_count": raw_count,
            "dim_written": dim_written,
            "fact_written": fact_written,
            "duplicates_skipped": dim_dups + fact_dups,
            "status": "success",
        }
