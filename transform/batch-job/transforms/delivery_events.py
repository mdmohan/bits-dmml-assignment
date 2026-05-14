from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp

from transforms.base import BaseTransformer
from transforms.common_dims import dim_customer_from_raw, dim_seller_from_raw


class DeliveryEventsTransformer(BaseTransformer):
    def build_dimensions(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        return {
            "mart.dim_customer": dim_customer_from_raw(raw_df),
            "mart.dim_seller": dim_seller_from_raw(raw_df),
        }

    def build_facts(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        fact_df = (
            self.with_common_event_columns(raw_df)
            .select(
                "event_id",
                "order_id",
                "event_type",
                col("payload.carrier_status").alias("carrier_status"),
                to_timestamp(col("payload.shipped_ts")).alias("shipped_ts"),
                to_timestamp(col("payload.delivered_customer_ts")).alias("delivered_customer_ts"),
                col("payload.delivery_delay_hours").cast("decimal(10,2)").alias("delivery_delay_hours"),
                "event_ts",
                "event_date",
                "seller_id",
                "customer_id",
                "source_system",
                "schema_version",
                "ingestion_ts",
            )
            .dropDuplicates(["event_id"])
        )
        return {"mart.fact_delivery_events": fact_df}
