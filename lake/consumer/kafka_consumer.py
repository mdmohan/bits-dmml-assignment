"""
kafka_consumer.py
Thin wrapper around confluent_kafka.Consumer with manual offset commit support.
"""
from __future__ import annotations

import logging
from typing import Iterable, List

from confluent_kafka import Consumer, KafkaException, TopicPartition

logger = logging.getLogger(__name__)


class OlistKafkaConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: Iterable[str],
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
        **extra_conf,
    ):
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 600000,
            **extra_conf,
        }
        self._consumer = Consumer(conf)
        self._topics = list(topics)
        self._consumer.subscribe(self._topics, on_assign=self._on_assign)
        logger.info(
            "Kafka consumer ready → %s | group=%s | topics=%s",
            bootstrap_servers, group_id, self._topics,
        )

    @staticmethod
    def _on_assign(consumer, partitions):
        logger.info(
            "Partitions assigned: %s",
            [(p.topic, p.partition) for p in partitions],
        )

    def poll_batch(self, max_records: int, timeout_sec: float) -> list:
        """Poll up to max_records messages, blocking up to timeout_sec total."""
        import time
        batch = []
        deadline = time.time() + timeout_sec
        while len(batch) < max_records:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            msg = self._consumer.poll(timeout=min(remaining, 1.0))
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue
            batch.append(msg)
        return batch

    def commit(self, messages: List) -> None:
        """Commit offsets for the highest message per (topic, partition)."""
        if not messages:
            return
        highest: dict[tuple, int] = {}
        for m in messages:
            tp = (m.topic(), m.partition())
            if m.offset() > highest.get(tp, -1):
                highest[tp] = m.offset()
        tps = [TopicPartition(t, p, off + 1) for (t, p), off in highest.items()]
        try:
            self._consumer.commit(offsets=tps, asynchronous=False)
            logger.debug("Committed offsets: %s", tps)
        except KafkaException as exc:
            logger.error("Commit failed: %s", exc)
            raise

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception as exc:
            logger.warning("Consumer close error: %s", exc)
