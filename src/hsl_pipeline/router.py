"""Route raw messages to the main topic or the dead-letter topic.

Kept independent of Kafka so it can be unit tested: ``send`` is any callable
``send(topic, key, value)``. In production it wraps ``confluent_kafka.Producer.produce``.
"""

from __future__ import annotations

import json
import time
from collections import Counter
from collections.abc import Callable

from .metrics import PipelineMetrics
from .schema import InvalidMessage, parse_hfp, to_json

Send = Callable[[str, str | None, str], None]


class MessageRouter:
    """Validates each raw message and sends it to exactly one place.

    ``metrics`` is optional so tests and scripts can run without a Prometheus server.
    ``clock`` returns the receive time; replay passes the event time so lag stays ~0.
    """

    def __init__(
        self,
        send: Send,
        topic: str,
        dlq_topic: str,
        metrics: PipelineMetrics | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self.send = send
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.metrics = metrics
        self.clock = clock
        self.stats: Counter[str] = Counter()

    def handle(self, mqtt_topic: str, payload: bytes | str) -> bool:
        """Return True if the message was valid and sent to the main topic."""
        try:
            event = parse_hfp(mqtt_topic, payload)
        except InvalidMessage as err:
            raw = payload.decode("utf-8", "replace") if isinstance(payload, bytes) else payload
            self.send(
                self.dlq_topic,
                None,
                json.dumps(
                    {
                        "reason": err.reason,
                        "mqtt_topic": mqtt_topic,
                        "payload": raw,
                        "failed_at": time.time(),
                    }
                ),
            )
            self.stats["dead_letter"] += 1
            if self.metrics:
                self.metrics.dead_letter()
            return False
        # Key by vehicle so all events of one vehicle land in the same partition, in order.
        self.send(self.topic, f"{event.operator}:{event.vehicle}", to_json(event))
        self.stats["valid"] += 1
        if self.metrics:
            self.metrics.valid(event.event_time.timestamp(), self.clock())
        return True


def kafka_sender(bootstrap: str) -> tuple[Send, Callable[[], None]]:
    """Create an idempotent Kafka producer and return (send, flush)."""
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,  # no duplicates from producer retries
            "acks": "all",
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )

    def send(topic: str, key: str | None, value: str) -> None:
        while True:
            try:
                producer.produce(topic, key=key, value=value)
                break
            except BufferError:  # local queue full: let it drain, then retry
                producer.poll(0.5)
        producer.poll(0)

    def flush() -> None:
        producer.flush(10)

    return send, flush
