from pyspark.sql import DataFrame, SparkSession


def read_jsonl(spark: SparkSession, path: str, schema=None) -> DataFrame:
    reader = spark.read
    if schema is not None:
        reader = reader.schema(schema)
    return reader.json(path)
