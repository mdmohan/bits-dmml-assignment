from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit


def dim_customer_from_raw(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df.select("customer_id")
        .where(col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
        .withColumn("customer_unique_id", lit(None).cast("string"))
        .withColumn("customer_city", lit(None).cast("string"))
        .withColumn("customer_state", lit(None).cast("string"))
        .withColumn("customer_zip_prefix", lit(None).cast("string"))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", lit(None).cast("timestamp"))
    )


def dim_seller_from_raw(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df.select("seller_id")
        .where(col("seller_id").isNotNull())
        .dropDuplicates(["seller_id"])
        .withColumn("seller_city", lit(None).cast("string"))
        .withColumn("seller_state", lit(None).cast("string"))
        .withColumn("seller_zip_prefix", lit(None).cast("string"))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", lit(None).cast("timestamp"))
    )
