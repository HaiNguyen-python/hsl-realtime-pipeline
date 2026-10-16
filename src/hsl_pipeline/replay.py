"""Replay recorded (or synthetic) raw HFP messages into Kafka.

Lets anyone run the whole pipeline without the live feed, and makes failure tests
repeatable: the same input always produces the same output.

Usage:
    hsl-replay data/sample/hfp_sample.jsonl --speed 20 --loop
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from .config import get_settings
from .metrics import start_metrics_server
from .router import MessageRouter, kafka_sender

log = logging.getLogger("hsl_pipeline.replay")


def _fixed_clock(ts: float) -> Callable[[], float]:
    return lambda: ts


def event_time(record: dict) -> float | None:
    try:
        return datetime.fromisoformat(json.loads(record["payload"])["VP"]["tst"]).timestamp()
    except (KeyError, TypeError, ValueError):
        return None


def replay(path: Path, router: MessageRouter, speed: float) -> int:
    """Send every line; sleep to keep the original spacing divided by ``speed``."""
    sent, first_event, start = 0, None, time.monotonic()
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            record = json.loads(line)
            ts = event_time(record)
            if speed > 0 and ts is not None:
                first_event = first_event or ts
                wait = (ts - first_event) / speed - (time.monotonic() - start)
                if wait > 0:
                    time.sleep(min(wait, 5))
            # Replayed events are old by definition: measure lag against the replayed
            # event time instead of the wall clock, so the lag metric stays meaningful.
            if ts is not None:
                router.clock = _fixed_clock(ts)
            router.handle(record["topic"], record["payload"])
            sent += 1
    return sent


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(prog="hsl-replay")
    parser.add_argument("path", type=Path)
    parser.add_argument("--speed", type=float, default=10.0, help="0 = as fast as possible")
    parser.add_argument("--loop", action="store_true", help="replay forever")
    args = parser.parse_args(argv)

    settings = get_settings()
    send, flush = kafka_sender(settings.kafka_bootstrap)
    metrics = start_metrics_server(settings.metrics_port)
    router = MessageRouter(send, settings.kafka_topic, settings.kafka_dlq_topic, metrics)
    while True:
        n = replay(args.path, router, args.speed)
        flush()
        log.info("replayed %d messages, stats %s", n, dict(router.stats))
        if not args.loop:
            break


if __name__ == "__main__":
    main()
