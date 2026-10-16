"""Parsing and validation of HSL HFP vehicle position ("vp") messages.

A raw MQTT message has a topic such as
    /hfp/v2/journey/ongoing/vp/tram/0040/00412/1009/1/Kirurgi/12:31/1130106/4/60;24/19/73/42
and a JSON payload {"VP": {...}}. We flatten it into one typed event.

Field notes (from the Digitransit HFP documentation, verify against the current docs):
- ``tst`` is the event time (ISO 8601, UTC); ``tsi`` the same as unix seconds.
- ``dl`` is the offset from the timetable in seconds; negative means behind schedule.
  We store ``delay_s = -dl`` so that a positive number means "late".
"""

from __future__ import annotations

import json
from datetime import datetime

from pydantic import BaseModel, Field, ValidationError, field_validator

# Index of each level in the topic after splitting on "/" (leading "" included).
TOPIC_MODE_INDEX = 6
VALID_MODES = {"bus", "tram", "train", "metro", "ferry", "ubus", "robot"}
# Rough bounding box of the HSL area; points outside are GPS errors.
LAT_RANGE = (59.9, 60.6)
LON_RANGE = (24.0, 25.6)


class VehicleEvent(BaseModel):
    event_id: str
    mode: str
    operator: int
    vehicle: int
    route: str
    line: str | None = None
    direction: int = Field(ge=1, le=2)
    event_time: datetime
    delay_s: int
    speed_ms: float | None = Field(default=None, ge=0, le=60)
    lat: float
    lon: float

    @field_validator("lat")
    @classmethod
    def lat_in_area(cls, v: float) -> float:
        if not LAT_RANGE[0] <= v <= LAT_RANGE[1]:
            raise ValueError(f"latitude {v} outside HSL area")
        return v

    @field_validator("lon")
    @classmethod
    def lon_in_area(cls, v: float) -> float:
        if not LON_RANGE[0] <= v <= LON_RANGE[1]:
            raise ValueError(f"longitude {v} outside HSL area")
        return v

    @field_validator("mode")
    @classmethod
    def known_mode(cls, v: str) -> str:
        if v not in VALID_MODES:
            raise ValueError(f"unknown transport mode {v!r}")
        return v


class InvalidMessage(Exception):
    """Raised for messages that should go to the dead-letter topic."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def mode_from_topic(topic: str) -> str:
    parts = topic.split("/")
    if len(parts) <= TOPIC_MODE_INDEX or parts[1] != "hfp":
        raise InvalidMessage(f"unexpected topic format: {topic}")
    return parts[TOPIC_MODE_INDEX]


def parse_hfp(topic: str, payload: bytes | str) -> VehicleEvent:
    """Parse one raw HFP message or raise InvalidMessage with a readable reason."""
    try:
        body = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as err:
        raise InvalidMessage(f"payload is not JSON: {err}") from err
    vp = body.get("VP") if isinstance(body, dict) else None
    if not isinstance(vp, dict):
        raise InvalidMessage("payload has no 'VP' object")

    missing = [
        k
        for k in ("oper", "veh", "tst", "tsi", "dl", "lat", "long", "route", "dir")
        if vp.get(k) is None
    ]
    if missing:
        raise InvalidMessage(f"missing fields: {', '.join(missing)}")

    try:
        return VehicleEvent(
            event_id=f"{vp['oper']}:{vp['veh']}:{vp['tsi']}",
            mode=mode_from_topic(topic),
            operator=vp["oper"],
            vehicle=vp["veh"],
            route=str(vp["route"]),
            line=vp.get("desi"),
            direction=int(vp["dir"]),
            event_time=vp["tst"],
            delay_s=-int(vp["dl"]),
            speed_ms=vp.get("spd"),
            lat=vp["lat"],
            lon=vp["long"],
        )
    except (ValidationError, ValueError, TypeError) as err:
        raise InvalidMessage(f"validation failed: {err}") from err


def to_json(event: VehicleEvent) -> str:
    """Serialise for Kafka; event_time as ISO-8601 UTC string."""
    return event.model_dump_json()
