from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from transforms.base import BaseTransformer
from transforms.common_dims import dim_customer_from_raw


class PaymentEventsTransformer(BaseTransformer):
    def build_dimensions(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        return {"mart.dim_customer": dim_customer_from_raw(raw_df)}

    def build_facts(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        fact_df = (
            self.with_common_event_columns(raw_df)
            .select(
                "event_id",
                "order_id",
                col("payload.payment_sequential").cast("int").alias("payment_sequential"),
                col("payload.payment_type").alias("payment_type"),
                col("payload.payment_installments").cast("int").alias("payment_installments"),
                col("payload.payment_value").cast("decimal(18,2)").alias("payment_value"),
                col("payload.payment_status").alias("payment_status"),
                "event_ts",
                "event_date",
                "customer_id",
                "source_system",
                "schema_version",
                "ingestion_ts",
            )
            .dropDuplicates(["event_id"])
        )
        return {"mart.fact_payments": fact_df}
