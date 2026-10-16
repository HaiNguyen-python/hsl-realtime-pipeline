"""Settings from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Settings:
    # HSL high-frequency positioning (HFP) feed. Docs: https://digitransit.fi/en/developers/
    mqtt_host: str = field(default_factory=lambda: _env("MQTT_HOST", "mqtt.hsl.fi"))
    mqtt_port: int = field(default_factory=lambda: int(_env("MQTT_PORT", "8883")))
    # Trams only by default: a few hundred messages per second, easy on a laptop.
    # Use /hfp/v2/journey/ongoing/vp/+/# for every mode.
    mqtt_topic: str = field(
        default_factory=lambda: _env("MQTT_TOPIC", "/hfp/v2/journey/ongoing/vp/tram/#")
    )
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:19092"))
    kafka_topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", "hsl.vehicle_positions"))
    kafka_dlq_topic: str = field(
        default_factory=lambda: _env("KAFKA_DLQ_TOPIC", "hsl.vehicle_positions.dlq")
    )
    postgres_dsn: str = field(
        default_factory=lambda: _env("POSTGRES_DSN", "postgresql://hsl:hsl@localhost:5432/hsl")
    )
    bronze_path: str = field(default_factory=lambda: _env("BRONZE_PATH", "data/bronze/positions"))
    checkpoint_dir: str = field(default_factory=lambda: _env("CHECKPOINT_DIR", "data/checkpoints"))
    watermark: str = field(default_factory=lambda: _env("WATERMARK", "2 minutes"))
    window: str = field(default_factory=lambda: _env("WINDOW", "5 minutes"))
    trigger: str = field(default_factory=lambda: _env("TRIGGER", "30 seconds"))
    metrics_port: int = field(default_factory=lambda: int(_env("METRICS_PORT", "9108")))


def get_settings() -> Settings:
    return Settings()
