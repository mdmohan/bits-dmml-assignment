from pyspark.sql import SparkSession


def build_spark(
    app_name: str = "olist-batch-elt",
    minio_endpoint: str = "minio:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin",
    minio_secure: str = "false",
) -> SparkSession:
    endpoint = minio_endpoint
    if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        scheme = "https" if str(minio_secure).lower() == "true" else "http"
        endpoint = f"{scheme}://{endpoint}"

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(minio_secure).lower())
        .getOrCreate()
    )
