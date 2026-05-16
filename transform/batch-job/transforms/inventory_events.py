from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp

from transforms.base import BaseTransformer


class InventoryEventsTransformer(BaseTransformer):
    def build_dimensions(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        dim_product = (
            raw_df
            .withColumn("product_id", col("payload.product_id").cast("string"))
            .select("product_id")
            .where(col("product_id").isNotNull() & (col("product_id") != ""))
            .dropDuplicates(["product_id"])
            .withColumn("product_category_name", lit(None).cast("string"))
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
        return {"mart.dim_product": dim_product}

    def build_facts(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        fact_df = (
            self.with_common_event_columns(raw_df)
            .withColumn("order_item_id", col("payload.order_item_id").cast("int"))
            .withColumn("product_id", col("payload.product_id").cast("string"))
            .withColumn("shipping_limit_date", to_timestamp(col("payload.shipping_limit_ts")))
            .withColumn("price", col("payload.price").cast("double"))
            .withColumn("freight_value", col("payload.freight_value").cast("double"))
            .select(
                "order_id",
                "order_item_id",
                "product_id",
                "seller_id",
                "shipping_limit_date",
                "price",
                "freight_value",
                "event_date",
            )
            .where(
                col("order_id").isNotNull() &
                col("order_item_id").isNotNull() &
                col("product_id").isNotNull() &
                (col("product_id") != "") &
                col("seller_id").isNotNull()
            )
            .dropDuplicates(["order_id", "order_item_id"])
        )
        return {"mart.fact_order_items": fact_df}

