import json
import sys
import types

from hsl_pipeline import replay
from hsl_pipeline.router import MessageRouter, kafka_sender
from hsl_pipeline.sample import generate


class FakeProducer:
    """Stands in for confluent_kafka.Producer; the first produce() hits a full buffer."""

    instances: list["FakeProducer"] = []

    def __init__(self, config):
        self.config, self.sent, self.polls, self.full_once = config, [], 0, True
        FakeProducer.instances.append(self)

    def produce(self, topic, key=None, value=None):
        if self.full_once:
            self.full_once = False
            raise BufferError("queue full")
        self.sent.append((topic, key, value))

    def poll(self, timeout):
        self.polls += 1

    def flush(self, timeout):
        return 0


def test_kafka_sender_is_idempotent_and_retries_full_buffer(monkeypatch):
    monkeypatch.setitem(
        sys.modules, "confluent_kafka", types.SimpleNamespace(Producer=FakeProducer)
    )
    send, flush = kafka_sender("broker:9092")
    send("topic", "k", "v")
    flush()
    producer = FakeProducer.instances[-1]
    assert producer.config["enable.idempotence"] is True
    assert producer.config["acks"] == "all"
    assert producer.sent == [("topic", "k", "v")]
    assert producer.polls >= 2  # drained the queue after BufferError, then normal poll


def test_replay_sends_every_record(tmp_path):
    records = generate(minutes=2, vehicles_per_route=1)
    path = tmp_path / "rec.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    sent = []
    router = MessageRouter(lambda t, k, v: sent.append(t), "main", "dlq")
    assert replay.replay(path, router, speed=0) == len(records)
    assert len(sent) == len(records)
    assert router.stats["valid"] + router.stats["dead_letter"] == len(records)


def test_event_time_handles_broken_payloads():
    assert replay.event_time({"payload": "{not json"}) is None
    assert replay.event_time({"payload": json.dumps({"VP": {"tst": "2026-10-15T07:00:00Z"}})})


def test_router_records_prometheus_metrics():
    from datetime import datetime

    from prometheus_client import CollectorRegistry
    from tests_helpers import TOPIC, payload

    from hsl_pipeline.metrics import PipelineMetrics

    registry = CollectorRegistry()
    event_ts = datetime.fromisoformat("2026-10-15T07:00:05+00:00").timestamp()
    router = MessageRouter(
        lambda *a: None, "main", "dlq", PipelineMetrics(registry), clock=lambda: event_ts + 3
    )
    router.handle(TOPIC, payload())
    router.handle(TOPIC, "{broken")

    def value(name, **labels):
        return registry.get_sample_value(name, labels or None)

    assert value("hsl_messages_total", status="valid") == 1
    assert value("hsl_messages_total", status="dead_letter") == 1
    assert value("hsl_event_lag_seconds_sum") == 3
    assert value("hsl_last_event_timestamp_seconds") == event_ts
