from __future__ import annotations

from abc import ABC, abstractmethod
import os
import math

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, input_file_name, coalesce, to_timestamp, current_timestamp, expr, regexp_extract, to_date

from spark.jdbc import DbWriter
from spark.metadata import (
    ensure_etl_checkpoint_table,
    get_topic_checkpoint,
    upsert_topic_checkpoint,
)
from spark.upsert_sql import STAGING_DDL, STAGING_MAP, UPSERT_SQL


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

    def _ensure_staging(self) -> None:
        for sql in STAGING_DDL.values():
            self.db_writer.exec_sql(sql)

    def write(self, datasets: dict[str, DataFrame]) -> tuple[int, int]:
        written = 0
        duplicates = 0
        self._ensure_staging()
        for table, df in datasets.items():
            if df is None:
                print(f"[{self.topic_key}] write table={table} skipped reason=df_is_none")
                continue
            total_before = df.count()
            print(f"[{self.topic_key}] write table={table} step=pre_dedupe rows={total_before}")
            df_new = self._idempotent_new_rows(table, df)
            total_after = df_new.count()
            duplicates += max(total_before - total_after, 0)
            print(
                f"[{self.topic_key}] write table={table} step=post_dedupe "
                f"rows={total_after} duplicates_skipped={max(total_before - total_after, 0)}"
            )
            if df_new.rdd.isEmpty():
                print(f"[{self.topic_key}] write table={table} skipped reason=no_new_rows")
                continue
            stg_table = STAGING_MAP.get(table)
            if stg_table is None:
                print(f"[{self.topic_key}] write table={table} skipped reason=no_staging_map")
                continue
            print(f"[{self.topic_key}] write table={table} step=staging_write stg_table={stg_table}")
            self.db_writer.write_append_jdbc(df_new, stg_table)
            print(f"[{self.topic_key}] write table={table} step=upsert_execute")
            self.db_writer.exec_sql(UPSERT_SQL[table])
            print(f"[{self.topic_key}] write table={table} step=upsert_done rows_written={total_after}")
            written += total_after
        print(f"[{self.topic_key}] write_summary rows_written={written} duplicates_skipped={duplicates}")
        return written, duplicates

    def run(self) -> dict[str, int]:
        ensure_etl_checkpoint_table(self.db_writer)
        checkpoint = get_topic_checkpoint(self.spark, self.db_writer, self.topic_key)
        print(f"[{self.topic_key}] source_path={self.source_path}")
        print(f"[{self.topic_key}] checkpoint={checkpoint}")

        raw_df = (
            self.read_raw()
            .withColumn("_source_file", input_file_name())
            .withColumn("_path_dt", to_date(regexp_extract(col("_source_file"), r"/dt=(\\d{4}-\\d{2}-\\d{2})/", 1)))
            .withColumn("_path_hour", regexp_extract(col("_source_file"), r"/(?:hour|hr)=(\\d{1,2})/", 1).cast("int"))
        )
        window_minutes = int(os.getenv("WINDOW_MINUTES", "20"))
        bootstrap_target_runs = int(os.getenv("BOOTSTRAP_TARGET_RUNS", "5"))
        bootstrap_min_batch = int(os.getenv("BOOTSTRAP_MIN_BATCH", "10000"))
        bootstrap_max_batch = int(os.getenv("BOOTSTRAP_MAX_BATCH", "200000"))
        raw_df = raw_df.withColumn(
            "_ingest_ts",
            coalesce(
                to_timestamp(col("_kafka_ingest_ts")),
                to_timestamp(col("_raw_file_ts")),
                to_timestamp(col("ingestion_hint_ts")),
            ),
        ).where(col("_ingest_ts").isNotNull())
        base_count = raw_df.count()
        print(f"[{self.topic_key}] base_rows_with_ingest_ts={base_count}")

        if checkpoint is None:
            # First run bootstrap: process full backlog in bounded chunks.
            pending_df = raw_df.orderBy(col("_path_dt").asc(), col("_path_hour").asc(), col("_ingest_ts").asc())
            pending_count = pending_df.count()
            print(f"[{self.topic_key}] mode=bootstrap pending_count={pending_count}")
            if pending_count == 0:
                return {
                    "raw_count": 0,
                    "dim_written": 0,
                    "fact_written": 0,
                    "duplicates_skipped": 0,
                    "status": "no_new_data",
                }
            dynamic_batch = int(math.ceil(pending_count / max(bootstrap_target_runs, 1)))
            batch_size = max(bootstrap_min_batch, min(dynamic_batch, bootstrap_max_batch))
            print(
                f"[{self.topic_key}] bootstrap_batch dynamic={dynamic_batch} "
                f"min={bootstrap_min_batch} max={bootstrap_max_batch} selected={batch_size}"
            )
            raw_df = pending_df.limit(batch_size)
        else:
            last_dt = checkpoint["last_dt"]
            last_hour = int(checkpoint["last_hour"] or 0)
            seen_keys = checkpoint["processed_object_keys"]
            print(
                f"[{self.topic_key}] mode=incremental last_dt={last_dt} "
                f"last_hour={last_hour} seen_keys_count={len(seen_keys)} window_minutes={window_minutes}"
            )

            newer_partitions = (
                (col("_path_dt") > expr(f"date '{last_dt.isoformat()}'")) |
                ((col("_path_dt") == expr(f"date '{last_dt.isoformat()}'")) & (col("_path_hour") > last_hour))
            )
            same_partition_unseen = (
                (col("_path_dt") == expr(f"date '{last_dt.isoformat()}'")) &
                (col("_path_hour") == last_hour) &
                (~col("_source_file").isin(list(seen_keys)) if seen_keys else expr("true"))
            )

            raw_df = raw_df.where(newer_partitions | same_partition_unseen)
            partition_filtered_count = raw_df.count()
            print(f"[{self.topic_key}] rows_after_partition_filter={partition_filtered_count}")
            raw_df = raw_df.where(col("_ingest_ts") >= (current_timestamp() - expr(f"INTERVAL {window_minutes} MINUTES")))
            window_filtered_count = raw_df.count()
            print(f"[{self.topic_key}] rows_after_window_filter={window_filtered_count}")
            raw_df = raw_df.orderBy(col("_path_dt").asc(), col("_path_hour").asc(), col("_ingest_ts").asc())

        raw_count = raw_df.count()
        print(f"[{self.topic_key}] rows_selected_for_processing={raw_count}")
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

        max_part = (
            raw_df.select("_path_dt", "_path_hour")
            .dropna()
            .orderBy(col("_path_dt").desc(), col("_path_hour").desc())
            .limit(1)
            .collect()
        )
        if max_part:
            max_dt = max_part[0]["_path_dt"]
            max_hour = int(max_part[0]["_path_hour"])
            keys_for_max = [
                r["_source_file"]
                for r in raw_df.where((col("_path_dt") == max_dt) & (col("_path_hour") == max_hour))
                .select("_source_file").dropDuplicates().collect()
            ]
            print(
                f"[{self.topic_key}] checkpoint_update last_dt={max_dt} "
                f"last_hour={max_hour} keys_for_partition={len(keys_for_max)}"
            )
            upsert_topic_checkpoint(self.db_writer, self.topic_key, max_dt, max_hour, keys_for_max)

        return {
            "raw_count": raw_count,
            "dim_written": dim_written,
            "fact_written": fact_written,
            "duplicates_skipped": dim_dups + fact_dups,
            "status": "success",
        }
