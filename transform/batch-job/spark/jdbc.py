from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class DbWriter:
    def __init__(self, url: str, user: str, password: str, db_type: str = "jdbc"):
        if db_type != "jdbc":
            raise ValueError("Only jdbc is supported")

        self._url = url
        self._user = user
        self._password = password

    def write_append_jdbc(self, df: DataFrame, table: str) -> None:
        (df.write
            .format("jdbc")
            .option("url", self._url)
            .option("dbtable", table)
            .option("user", self._user)
            .option("password", self._password)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save())

    def read_table(self, spark: SparkSession, table: str) -> DataFrame:
        return (
            spark.read
            .format("jdbc")
            .option("url", self._url)
            .option("dbtable", table)
            .option("user", self._user)
            .option("password", self._password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    def exec_sql(self, sql: str) -> None:
        import psycopg2
        from urllib.parse import urlparse

        parsed = urlparse(self._url.replace("jdbc:", "", 1))
        dbname = (parsed.path or "/postgres").lstrip("/")
        host = parsed.hostname or "localhost"
        port = parsed.port or 5432

        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=self._user,
            password=self._password,
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
        finally:
            conn.close()
