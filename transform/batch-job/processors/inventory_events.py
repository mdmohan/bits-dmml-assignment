from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, current_timestamp, lit
import os

from processors.base import BaseEventProcessor
from transforms.inventory_events import InventoryEventsTransformer
from utils.readers import read_jsonl


class InventoryEventsProcessor(BaseEventProcessor):
    topic_key = "inventory_events"

    def __init__(self, spark, db_writer, source_path):
        super().__init__(spark, db_writer, source_path)
        self.transformer = InventoryEventsTransformer()
        bucket = os.getenv("MINIO_BUCKET", "datalake")
        self.product_details_path = os.getenv(
            "PRODUCT_DETAILS_PATH",
            f"s3a://{bucket}/reference/product_details/_latest/product_details.jsonl",
        )

    def read_raw(self) -> DataFrame:
        return read_jsonl(self.spark, self.source_path)

    def build_dimensions(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        dims = self.transformer.build_dimensions(raw_df)
        product_from_events = dims["mart.dim_product"]
        try:
            print(f"[{self.topic_key}] loading_product_sideload path={self.product_details_path}")
            product_details = self.spark.read.json(self.product_details_path)
            product_from_ref = (
                product_details
                .select(
                    col("product_id").cast("string").alias("product_id"),
                    col("product_category").cast("string").alias("product_category_name"),
                )
                .where(col("product_id").isNotNull() & (col("product_id") != ""))
                .dropDuplicates(["product_id"])
                .withColumn("product_name_lenght", lit(None).cast("int"))
                .withColumn("product_description_lenght", lit(None).cast("int"))
                .withColumn("product_photos_qty", lit(None).cast("int"))
                .withColumn("product_weight_g", lit(None).cast("int"))
                .withColumn("product_length_cm", lit(None).cast("int"))
                .withColumn("product_height_cm", lit(None).cast("int"))
                .withColumn("product_width_cm", lit(None).cast("int"))
                .withColumn("created_at", current_timestamp())
                .withColumn("updated_at", lit(None).cast("timestamp"))
            )
            dims["mart.dim_product"] = (
                product_from_ref.unionByName(product_from_events)
                .dropDuplicates(["product_id"])
            )
        except Exception as exc:
            print(f"[{self.topic_key}] product_sideload_unavailable fallback=events_only reason={exc}")
            dims["mart.dim_product"] = product_from_events
        return dims

    def build_facts(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        facts = self.transformer.build_facts(raw_df)
        foi = facts["mart.fact_order_items"]
        self._debug(f"inventory_fact_initial_rows={foi.count()}")
        self._debug(f"inventory_fact_null_seller_initial={foi.where(col('seller_id').isNull()).count()}")
        try:
            order_seller = (
                self.db_writer.read_table(self.spark, "mart.fact_order_events")
                .select("order_id", "seller_id")
                .where(col("order_id").isNotNull() & col("seller_id").isNotNull())
                .dropDuplicates(["order_id"])
                .withColumnRenamed("seller_id", "seller_id_from_order")
            )
            self._debug(f"order_seller_lookup_rows={order_seller.count()}")
            foi = (
                foi.join(order_seller, on="order_id", how="left")
                .withColumn("seller_id", coalesce(col("seller_id"), col("seller_id_from_order")))
                .drop("seller_id_from_order")
                .where(col("seller_id").isNotNull())
            )
            self._debug(f"inventory_fact_after_backfill_rows={foi.count()}")
        except Exception as exc:
            print(f"[{self.topic_key}] seller_backfill skipped: {exc}")
            foi = foi.where(col("seller_id").isNotNull())
        self._debug(f"inventory_fact_final_rows={foi.count()}")

        facts["mart.fact_order_items"] = foi
        return facts
