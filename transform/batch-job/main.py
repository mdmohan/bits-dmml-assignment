import os
import json

from processors.delivery_events import DeliveryEventsProcessor
from processors.order_events import OrderEventsProcessor
from processors.payment_events import PaymentEventsProcessor
from processors.review_events import ReviewEventsProcessor
from spark.jdbc import DbWriter
from spark.spark_session import build_spark
from utils.config import load_topics_config, minio_topic_path


def main() -> None:
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_bucket = os.getenv("MINIO_BUCKET", "olist-temp")
    minio_secure = os.getenv("MINIO_SECURE", "false")

    spark = build_spark(
        app_name="olist-batch-elt",
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
        minio_secure=minio_secure,
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.ui.showConsoleProgress", "false")
    log4j = spark._jvm.org.apache.log4j
    log4j.LogManager.getLogger("org").setLevel(log4j.Level.ERROR)
    log4j.LogManager.getLogger("akka").setLevel(log4j.Level.ERROR)

    cfg = load_topics_config(os.getenv("TOPICS_CONFIG"))
    cfg["minio"]["bucket"] = minio_bucket

    db_writer = DbWriter(
        url=os.getenv("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/postgres"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )

    processors = [
        OrderEventsProcessor(spark, db_writer, minio_topic_path(cfg, "order_events")),
        PaymentEventsProcessor(spark, db_writer, minio_topic_path(cfg, "payment_events")),
        DeliveryEventsProcessor(spark, db_writer, minio_topic_path(cfg, "delivery_events")),
        ReviewEventsProcessor(spark, db_writer, minio_topic_path(cfg, "review_events")),
    ]

    enabled = set(filter(None, os.getenv("ENABLED_PROCESSORS", "order_events").split(",")))
    run_summary = {
        "processors_run": 0,
        "raw_count": 0,
        "dim_written": 0,
        "fact_written": 0,
        "duplicates_skipped": 0,
        "no_new_data_processors": 0,
    }

    for processor in processors:
        if processor.topic_key not in enabled:
            continue
        print(f"Running processor: {processor.topic_key}")
        metrics = processor.run()
        print(f"Metrics[{processor.topic_key}] => {metrics}")
        run_summary["processors_run"] += 1
        run_summary["raw_count"] += int(metrics.get("raw_count", 0))
        run_summary["dim_written"] += int(metrics.get("dim_written", 0))
        run_summary["fact_written"] += int(metrics.get("fact_written", 0))
        run_summary["duplicates_skipped"] += int(metrics.get("duplicates_skipped", 0))
        if metrics.get("status") == "no_new_data":
            run_summary["no_new_data_processors"] += 1

    spark.stop()
    print("=== Spark Batch Summary ===")
    print(f"processors_run={run_summary['processors_run']}")
    print(f"raw_events_read={run_summary['raw_count']}")
    print(f"rows_written_dims={run_summary['dim_written']}")
    print(f"rows_written_facts={run_summary['fact_written']}")
    print(f"duplicates_skipped={run_summary['duplicates_skipped']}")
    print(f"processors_no_new_data={run_summary['no_new_data_processors']}")
    print("Batch job complete")
    print(f"METRICS_JSON={json.dumps(run_summary, sort_keys=True)}")


if __name__ == "__main__":
    main()
