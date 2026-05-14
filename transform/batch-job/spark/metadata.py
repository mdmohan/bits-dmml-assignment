from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, max as spark_max

from spark.jdbc import DbWriter


def ensure_etl_watermark_table(db_writer: DbWriter) -> None:
    ddl = """
    create table if not exists mart.etl_watermark (
      topic_key text primary key,
      last_event_ts timestamp,
      processed_object_keys text,
      updated_at timestamp default current_timestamp
    );
    """
    db_writer.exec_sql(ddl)


def get_topic_watermark(spark: SparkSession, db_writer: DbWriter, topic_key: str):
    try:
        df = db_writer.read_table(spark, "mart.etl_watermark").where(col("topic_key") == topic_key)
        if df.rdd.isEmpty():
            return None, set()
        row = df.collect()[0]
        last_ts = row["last_event_ts"]
        keys_raw = row["processed_object_keys"] or "[]"
        keys = set(json.loads(keys_raw))
        return last_ts, keys
    except Exception:
        return None, set()


def upsert_topic_watermark(
    db_writer: DbWriter,
    topic_key: str,
    last_event_ts,
    processed_object_keys: list[str],
) -> None:
    # keep key list bounded for practicality
    trimmed = processed_object_keys[-5000:]
    keys_json = json.dumps(trimmed)

    last_ts_sql = "null"
    if last_event_ts is not None:
        if hasattr(last_event_ts, "to_pydatetime"):
            last_event_ts = last_event_ts.to_pydatetime()
        if isinstance(last_event_ts, datetime):
            if last_event_ts.tzinfo is None:
                last_event_ts = last_event_ts.replace(tzinfo=timezone.utc)
            last_ts_sql = f"'{last_event_ts.isoformat()}'::timestamp"

    sql = f"""
    insert into mart.etl_watermark(topic_key, last_event_ts, processed_object_keys, updated_at)
    values ('{topic_key}', {last_ts_sql}, '{keys_json.replace("'", "''")}', current_timestamp)
    on conflict (topic_key)
    do update set
      last_event_ts = excluded.last_event_ts,
      processed_object_keys = excluded.processed_object_keys,
      updated_at = current_timestamp;
    """
    db_writer.exec_sql(sql)


def max_event_ts(df: DataFrame):
    if df.rdd.isEmpty():
        return None
    row = df.select(spark_max(col("event_ts")).alias("mx")).collect()[0]
    return row["mx"]
