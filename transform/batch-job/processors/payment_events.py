from pyspark.sql import DataFrame

from processors.base import BaseEventProcessor
from transforms.payment_events import PaymentEventsTransformer
from utils.readers import read_jsonl


class PaymentEventsProcessor(BaseEventProcessor):
    topic_key = "payment_events"

    def __init__(self, spark, db_writer, source_path):
        super().__init__(spark, db_writer, source_path)
        self.transformer = PaymentEventsTransformer()

    def read_raw(self) -> DataFrame:
        return read_jsonl(self.spark, self.source_path)

    def build_dimensions(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        return self.transformer.build_dimensions(raw_df)

    def build_facts(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        return self.transformer.build_facts(raw_df)
