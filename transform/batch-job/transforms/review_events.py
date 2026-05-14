from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, lit

from transforms.base import BaseTransformer
from transforms.common_dims import dim_customer_from_raw, dim_seller_from_raw


class ReviewEventsTransformer(BaseTransformer):
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
                col("payload.review_id").alias("review_id"),
                "order_id",
                col("payload.review_score").cast("int").alias("review_score"),
                to_timestamp(col("payload.review_created_ts")).alias("review_created_ts"),
                to_timestamp(col("payload.review_answer_ts")).alias("review_answer_ts"),
                lit(None).cast("decimal(10,2)").alias("review_response_hours"),
                col("payload.review_comment_title").alias("review_comment_title"),
                "event_ts",
                "event_date",
                "customer_id",
                "seller_id",
                "source_system",
                "schema_version",
                "ingestion_ts",
            )
            .dropDuplicates(["event_id"])
        )
        return {"mart.fact_reviews": fact_df}
