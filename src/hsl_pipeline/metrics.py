"""Prometheus metrics for the producer and replay services.

Exposed at http://<host>:9108/metrics and scraped by Prometheus (see docker-compose.yml).
What each metric answers:
- hsl_messages_total{status}        how many messages were valid vs dead-lettered
- hsl_event_lag_seconds             how old an event is when we receive it (feed health)
- hsl_last_event_timestamp_seconds  is data still flowing? (alert if it stops moving)
"""

from __future__ import annotations

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram


class PipelineMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY):
        self.messages = Counter(
            "hsl_messages_total", "Messages handled by the producer", ["status"], registry=registry
        )
        self.event_lag = Histogram(
            "hsl_event_lag_seconds",
            "Seconds between the vehicle's GPS timestamp and our receive time",
            buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300, 900),
            registry=registry,
        )
        self.last_event = Gauge(
            "hsl_last_event_timestamp_seconds",
            "Event time (unix seconds) of the newest valid message",
            registry=registry,
        )

    def valid(self, event_ts: float, received_ts: float) -> None:
        self.messages.labels(status="valid").inc()
        self.event_lag.observe(max(0.0, received_ts - event_ts))
        self.last_event.set(event_ts)

    def dead_letter(self) -> None:
        self.messages.labels(status="dead_letter").inc()


def start_metrics_server(port: int) -> PipelineMetrics:
    from prometheus_client import start_http_server

    start_http_server(port)
    return PipelineMetrics()
