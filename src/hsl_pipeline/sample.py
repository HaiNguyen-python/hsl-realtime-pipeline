"""Generate a synthetic HFP-like recording for demos and tests.

It is NOT real HSL data. It imitates the message format and includes the messy parts a
real feed has: duplicate messages, late arrivals, GPS errors and broken payloads.
Record the real feed with ``hsl-producer --record`` for real data.
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROUTES = ["1001", "1002", "1003", "1004", "1006", "1007", "1009", "1010", "1013", "1015"]
CENTER = (60.170, 24.941)


def _message(route: str, veh: int, ts: datetime, dl: int, rng: random.Random) -> dict:
    lat = CENTER[0] + rng.uniform(-0.03, 0.03)
    lon = CENTER[1] + rng.uniform(-0.06, 0.06)
    topic = (
        f"/hfp/v2/journey/ongoing/vp/tram/0040/{veh:05d}/{route}/1/Dest/"
        f"{ts:%H:%M}/1130106/4/60;24/19/73/42"
    )
    vp = {
        "desi": route[-2:].lstrip("0"),
        "dir": "1",
        "oper": 40,
        "veh": veh,
        "tst": ts.isoformat().replace("+00:00", "Z"),
        "tsi": int(ts.timestamp()),
        "spd": round(rng.uniform(0, 12), 2),
        "hdg": rng.randint(0, 359),
        "lat": round(lat, 6),
        "long": round(lon, 6),
        "dl": dl,
        "route": route,
        "oday": ts.date().isoformat(),
        "loc": "GPS",
    }
    return {"topic": topic, "payload": json.dumps({"VP": vp})}


def generate(
    minutes: int = 30,
    vehicles_per_route: int = 3,
    seed: int = 1,
    start: datetime | None = None,
    late_burst: bool = True,
) -> list[dict]:
    rng = random.Random(seed)
    start = start or datetime(2026, 10, 15, 7, 0, tzinfo=UTC)
    records: list[tuple[datetime, dict]] = []
    for r_i, route in enumerate(ROUTES):
        for v in range(vehicles_per_route):
            veh = 400 + r_i * 10 + v
            delay = rng.randint(-30, 60)  # seconds late; random walk per vehicle
            for sec in range(0, minutes * 60, 5):  # one message every 5 seconds
                ts = start + timedelta(seconds=sec)
                # Route 1004 builds up a jam in the second half of the window.
                drift = 3 if route == "1004" and sec > minutes * 30 else 0
                delay = max(-120, delay + rng.randint(-2, 2) + drift)
                records.append((ts, _message(route, veh, ts, -delay, rng)))

    out: list[dict] = []
    for _ts, rec in sorted(records, key=lambda x: x[0]):
        out.append(rec)
        roll = rng.random()
        if roll < 0.01:  # duplicate delivery
            out.append(rec)
        elif roll < 0.015:  # GPS error -> dead letter
            bad = json.loads(rec["payload"])
            bad["VP"]["lat"] = 0.0
            out.append({"topic": rec["topic"], "payload": json.dumps(bad)})
        elif roll < 0.017:  # broken payload -> dead letter
            out.append({"topic": rec["topic"], "payload": "{not json"})
    # A burst of very late events (10 minutes old) at the end, to exercise the watermark.
    if late_burst:
        late_ts = start + timedelta(minutes=minutes - 10)
        for i in range(20):
            out.append(_message("1001", 499, late_ts + timedelta(seconds=i), -600, rng))
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="hsl-sample")
    parser.add_argument("--out", type=Path, default=Path("data/sample/hfp_sample.jsonl"))
    parser.add_argument("--minutes", type=int, default=30)
    args = parser.parse_args(argv)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    records = generate(args.minutes)
    with args.out.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")
    print(f"wrote {len(records)} messages to {args.out}")


if __name__ == "__main__":
    main()
