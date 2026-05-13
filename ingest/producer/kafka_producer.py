"""
kafka_producer.py
Thin wrapper around confluent_kafka.Producer with delivery confirmation logging.
"""
import json
import logging
from typing import Any, Dict

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class OlistKafkaProducer:
    def __init__(self, bootstrap_servers: str, **extra_conf):
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "acks":              "all",
            "retries":           3,
            "retry.backoff.ms":  500,
            "compression.type":  "snappy",
            **extra_conf,
        }
        self._producer = Producer(conf)
        logger.info("Kafka producer ready → %s", bootstrap_servers)

    def publish(self, topic: str, key: str, value: Dict[str, Any]) -> None:
        """Non-blocking publish; delivery result is logged via callback."""
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(value, default=str).encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)  # fire pending callbacks without blocking

    def flush(self, timeout: float = 15.0) -> None:
        """Block until all queued messages are delivered (or timeout)."""
        remaining = self._producer.flush(timeout)
        if remaining:
            logger.warning("%d message(s) not confirmed before flush timeout", remaining)
        else:
            logger.info("All messages flushed successfully.")

    @staticmethod
    def _on_delivery(err, msg):
        if err:
            logger.error(
                "Delivery FAILED | topic=%s key=%s err=%s",
                msg.topic(), msg.key(), err,
            )
        else:
            logger.debug(
                "Delivered | topic=%s partition=%d offset=%d key=%s",
                msg.topic(), msg.partition(), msg.offset(), msg.key().decode(),
            )
