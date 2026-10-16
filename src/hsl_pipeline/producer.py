"""Live producer: subscribe to the HSL MQTT feed and publish events to Kafka.

Usage:
    hsl-producer                      # live feed -> Kafka
    hsl-producer --record data/recordings/tram.jsonl --minutes 60
                                      # also save raw messages for later replay
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import ssl
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt

from .config import get_settings
from .metrics import start_metrics_server
from .router import MessageRouter, kafka_sender

log = logging.getLogger("hsl_pipeline.producer")


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(prog="hsl-producer")
    parser.add_argument("--record", type=Path, help="also append raw messages to this JSONL file")
    parser.add_argument("--minutes", type=float, default=0, help="stop after N minutes (0 = run)")
    args = parser.parse_args(argv)

    settings = get_settings()
    send, flush = kafka_sender(settings.kafka_bootstrap)
    metrics = start_metrics_server(settings.metrics_port)
    router = MessageRouter(send, settings.kafka_topic, settings.kafka_dlq_topic, metrics)
    record_file = None
    if args.record:
        args.record.parent.mkdir(parents=True, exist_ok=True)
        record_file = args.record.open("a", encoding="utf-8")

    def on_connect(client, userdata, flags, reason_code, properties):
        log.info(
            "connected to %s (%s), subscribing %s",
            settings.mqtt_host,
            reason_code,
            settings.mqtt_topic,
        )
        client.subscribe(settings.mqtt_topic, qos=0)

    def on_message(client, userdata, msg):
        router.handle(msg.topic, msg.payload)
        if record_file:
            record_file.write(
                json.dumps({"topic": msg.topic, "payload": msg.payload.decode("utf-8", "replace")})
                + "\n"
            )

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if settings.mqtt_port == 8883:
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=60)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    deadline = time.monotonic() + args.minutes * 60 if args.minutes else None

    client.loop_start()
    try:
        while not stop.is_set() and (deadline is None or time.monotonic() < deadline):
            stop.wait(30)
            log.info("stats %s", dict(router.stats))
    finally:
        client.loop_stop()
        client.disconnect()
        flush()
        if record_file:
            record_file.close()
        log.info("stopped, final stats %s", dict(router.stats))


if __name__ == "__main__":
    main()
