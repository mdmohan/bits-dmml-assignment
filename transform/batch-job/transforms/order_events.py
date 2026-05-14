from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp

from transforms.base import BaseTransformer
from transforms.common_dims import dim_customer_from_raw, dim_seller_from_raw


class OrderEventsTransformer(BaseTransformer):
    def build_dimensions(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        return {
            "mart.dim_customer": dim_customer_from_raw(raw_df),
            "mart.dim_seller": dim_seller_from_raw(raw_df),
        }

    def build_facts(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        fact_df = (
            self.with_common_event_columns(raw_df)
            .withColumn("order_status", col("payload.order_status"))
            .withColumn("purchase_ts", to_timestamp(col("payload.purchase_ts")))
            .withColumn("approved_ts", to_timestamp(col("payload.approved_ts")))
            .withColumn("estimated_delivery_ts", to_timestamp(col("payload.estimated_delivery_ts")))
            .select(
                "event_id", "order_id", "event_type", "event_ts", "event_date",
                "customer_id", "seller_id", "order_status", "purchase_ts", "approved_ts",
                "estimated_delivery_ts", "source_system", "schema_version", "ingestion_ts"
            )
            .dropDuplicates(["event_id"])
        )
        return {"mart.fact_order_events": fact_df}
