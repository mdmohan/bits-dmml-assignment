from __future__ import annotations

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp


class BaseTransformer(ABC):
    @staticmethod
    def with_common_event_columns(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("event_ts", to_timestamp(col("event_ts")))
            .withColumn("event_date", to_date(col("event_ts")))
            .withColumn("ingestion_ts", current_timestamp())
        )

    @abstractmethod
    def build_dimensions(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        pass

    @abstractmethod
    def build_facts(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        pass
