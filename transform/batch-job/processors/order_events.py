from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
import os

from processors.base import BaseEventProcessor
from transforms.order_events import OrderEventsTransformer
from utils.readers import read_jsonl


class OrderEventsProcessor(BaseEventProcessor):
    topic_key = "order_events"

    def __init__(self, spark, db_writer, source_path):
        super().__init__(spark, db_writer, source_path)
        self.transformer = OrderEventsTransformer()
        bucket = os.getenv("MINIO_BUCKET", "datalake")
        self.customer_details_path = os.getenv(
            "CUSTOMER_DETAILS_PATH",
            f"s3a://{bucket}/reference/customer_details/_latest/customer_details.jsonl",
        )

    def read_raw(self) -> DataFrame:
        return read_jsonl(self.spark, self.source_path)

    def build_dimensions(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        dims = self.transformer.build_dimensions(raw_df)
        customer_from_events = dims["mart.dim_customer"]
        try:
            print(f"[{self.topic_key}] loading_customer_sideload path={self.customer_details_path}")
            customer_details = self.spark.read.json(self.customer_details_path)
            customer_from_ref = (
                customer_details
                .select(
                    col("customer_id").cast("string").alias("customer_id"),
                    col("customer_unique_id").cast("string").alias("customer_unique_id"),
                    col("city").cast("string").alias("customer_city"),
                    col("state").cast("string").alias("customer_state"),
                    lit(None).cast("string").alias("customer_zip_prefix"),
                )
                .where(col("customer_id").isNotNull() & (col("customer_id") != ""))
                .dropDuplicates(["customer_id"])
                .withColumn("created_at", current_timestamp())
                .withColumn("updated_at", lit(None).cast("timestamp"))
            )
            dims["mart.dim_customer"] = (
                customer_from_ref.unionByName(customer_from_events)
                .dropDuplicates(["customer_id"])
            )
        except Exception as exc:
            print(f"[{self.topic_key}] customer_sideload_unavailable fallback=events_only reason={exc}")
            dims["mart.dim_customer"] = customer_from_events
        return dims

    def build_facts(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        return self.transformer.build_facts(raw_df)
