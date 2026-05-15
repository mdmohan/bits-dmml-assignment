from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType

import os



def env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    if value == "":
        raise ValueError(f"Missing required env var: {name}")
    return value

# 1) Spark session with S3A (MinIO) config
spark = (
    SparkSession.builder
    .appName("minio-event-transform-sample")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", env("MINIO_ACCESS_KEY", "minioadmin"))
    .config("spark.hadoop.fs.s3a.secret.key", env("MINIO_SECRET_KEY", "minioadmin"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# 2) Define input schema (raw event envelope)
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_ts", StringType(), False),      # parse later
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("source_system", StringType(), False),
    StructField("schema_version", StringType(), False),
])

# 3) Read JSON events from MinIO bucket/path
raw_df = spark.read.schema(event_schema).json(
    "s3a://olist-temp/raw/olist.order_events/dt=*/hr=*/*.jsonl"
)

# 4) Transform to mart.fact_order_events shape
fact_df = (
    raw_df
    .withColumn("event_ts", to_timestamp(col("event_ts")))
    .withColumn("event_date", to_date(col("event_ts")))
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("order_status", lit(None).cast("string"))
    .withColumn("purchase_ts", lit(None).cast("timestamp"))
    .withColumn("approved_ts", lit(None).cast("timestamp"))
    .withColumn("estimated_delivery_ts", lit(None).cast("timestamp"))
    .select(
        "event_id", "order_id", "event_type", "event_ts", "event_date",
        "customer_id", "seller_id", "order_status",
        "purchase_ts", "approved_ts", "estimated_delivery_ts",
        "source_system", "schema_version", "ingestion_ts"
    )
)

fact_df.show(10, truncate=False)
print(f"raw_count={raw_df.count()} transformed_count={fact_df.count()}")

# 5) Write dimensions first (to satisfy FK constraints), then facts
jdbc_url = env("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/postgres")
pg_user = env("PG_USER", "postgres")
pg_password = env("PG_PASSWORD", "postgres")
pg_table = env("PG_TABLE", "mart.fact_order_events")
pg_dim_customer = env("PG_DIM_CUSTOMER_TABLE", "mart.dim_customer")
pg_dim_seller = env("PG_DIM_SELLER_TABLE", "mart.dim_seller")

pg_props = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver",
}

dim_customer_df = (
    fact_df
    .select("customer_id")
    .where(col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
)

dim_seller_df = (
    fact_df
    .select("seller_id")
    .where(col("seller_id").isNotNull())
    .dropDuplicates(["seller_id"])
)

existing_customer_df = spark.read.jdbc(
    url=jdbc_url, table=pg_dim_customer, properties=pg_props
).select("customer_id")
new_customer_df = dim_customer_df.join(existing_customer_df, on="customer_id", how="left_anti")

existing_seller_df = spark.read.jdbc(
    url=jdbc_url, table=pg_dim_seller, properties=pg_props
).select("seller_id")
new_seller_df = dim_seller_df.join(existing_seller_df, on="seller_id", how="left_anti")

new_customer_df = (
    new_customer_df
    .withColumn("customer_unique_id", lit(None).cast("string"))
    .withColumn("customer_city", lit(None).cast("string"))
    .withColumn("customer_state", lit(None).cast("string"))
    .withColumn("customer_zip_prefix", lit(None).cast("string"))
    .withColumn("created_at", current_timestamp())
    .withColumn("updated_at", lit(None).cast("timestamp"))
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "customer_zip_prefix",
        "created_at",
        "updated_at",
    )
)

new_seller_df = (
    new_seller_df
    .withColumn("seller_city", lit(None).cast("string"))
    .withColumn("seller_state", lit(None).cast("string"))
    .withColumn("seller_zip_prefix", lit(None).cast("string"))
    .withColumn("created_at", current_timestamp())
    .withColumn("updated_at", lit(None).cast("timestamp"))
    .select(
        "seller_id",
        "seller_city",
        "seller_state",
        "seller_zip_prefix",
        "created_at",
        "updated_at",
    )
)

if new_customer_df.count() > 0:
    (new_customer_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", pg_dim_customer)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())

if new_seller_df.count() > 0:
    (new_seller_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", pg_dim_seller)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())

(fact_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", pg_table)
    .option("user", pg_user)
    .option("password", pg_password)
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save())

print(f"Wrote dimensions and transformed rows to {pg_table}")

spark.stop()
