from __future__ import annotations

import json
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from spark.jdbc import DbWriter


def ensure_etl_checkpoint_table(db_writer: DbWriter) -> None:
    ddl = """
    create table if not exists mart.etl_checkpoint (
      topic_key text primary key,
      last_dt date,
      last_hour int,
      processed_object_keys jsonb not null default '[]'::jsonb,
      updated_at timestamp not null default current_timestamp
    );
    """
    db_writer.exec_sql(ddl)


def get_topic_checkpoint(spark: SparkSession, db_writer: DbWriter, topic_key: str):
    try:
        df = db_writer.read_table(spark, "mart.etl_checkpoint").where(col("topic_key") == topic_key)
        if df.rdd.isEmpty():
            return None
        row = df.collect()[0]
        keys = row["processed_object_keys"] or []
        if isinstance(keys, str):
            keys = json.loads(keys)
        return {
            "last_dt": row["last_dt"],
            "last_hour": row["last_hour"],
            "processed_object_keys": set(keys),
        }
    except Exception:
        return None


def upsert_topic_checkpoint(
    db_writer: DbWriter,
    topic_key: str,
    last_dt: date,
    last_hour: int,
    processed_object_keys: list[str],
) -> None:
    keys_json = json.dumps(processed_object_keys[-10000:]).replace("'", "''")
    dt_sql = f"'{last_dt.isoformat()}'::date" if last_dt else "null"
    hour_sql = str(int(last_hour)) if last_hour is not None else "null"

    sql = f"""
    insert into mart.etl_checkpoint(topic_key, last_dt, last_hour, processed_object_keys, updated_at)
    values ('{topic_key}', {dt_sql}, {hour_sql}, '{keys_json}'::jsonb, current_timestamp)
    on conflict (topic_key)
    do update set
      last_dt = excluded.last_dt,
      last_hour = excluded.last_hour,
      processed_object_keys = excluded.processed_object_keys,
      updated_at = current_timestamp;
    """
    db_writer.exec_sql(sql)
