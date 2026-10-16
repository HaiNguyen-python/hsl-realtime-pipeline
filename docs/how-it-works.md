# How it works

This page follows one vehicle position message from the HSL feed to the Grafana dashboard.
File references point to `src/hsl_pipeline/`.

## 1. The raw message

HSL publishes every vehicle's position about once per second over MQTT. One message:

```
topic:   /hfp/v2/journey/ongoing/vp/tram/0040/00412/1009/1/Kirurgi/12:31/1130106/4/60;24/19/73/42
payload: {"VP": {"desi": "9", "dir": "1", "oper": 40, "veh": 412, "tst": "2026-10-15T07:00:05Z",
                 "tsi": 1792047605, "spd": 5.2, "lat": 60.17, "long": 24.94, "dl": -45,
                 "route": "1009", ...}}
```

The transport mode (`tram`) is the 7th level of the topic. `dl` is the offset from the timetable
in seconds; negative means behind schedule.

## 2. Validate at the edge (`schema.py`)

`parse_hfp(topic, payload)` turns the message into a typed `VehicleEvent` (pydantic) or raises
`InvalidMessage` with a readable reason:

- the payload must be JSON with a `VP` object and all required fields;
- latitude and longitude must be inside the Helsinki region (GPS errors often report 0, 0);
- speed must be between 0 and 60 m/s; the mode must be a known transport mode.

It also builds `event_id = operator:vehicle:timestamp`, used later to drop duplicates, and stores
`delay_s = -dl` so that a positive number means "late".

## 3. Route to Kafka (`router.py`)

`MessageRouter.handle` sends each message to exactly one place:

- valid → topic `hsl.vehicle_positions`, **keyed by vehicle**, so all events of one vehicle land in
  the same partition and stay in order;
- invalid → dead-letter topic `hsl.vehicle_positions.dlq`, with the reason and the raw payload.

The Kafka producer (`kafka_sender`) is configured with `enable.idempotence=True` and `acks=all`,
so producer retries never create duplicates and a write is acknowledged only when it is safe. When
its local queue is full it waits and retries instead of dropping data.

`metrics.py` counts valid and dead-lettered messages and measures **event lag** (receive time
minus GPS time) for Prometheus.

`producer.py` runs this on the live MQTT feed (and can record it to a file); `replay.py` plays a
recording or the synthetic sample (`sample.py`) back at N× speed, so the whole system runs offline.

## 4. Stream processing (`streaming_job.py`)

The Spark Structured Streaming job reads the topic and runs two queries:

**Bronze (all events).** `parse_events` turns the Kafka `value` into typed columns. Every event is
written as Parquet, partitioned by date and mode. Spark's file sink is exactly-once together with
the checkpoint.

**Delay windows (aggregates).** `delay_windows` does three things, in this order:

1. `withWatermark("event_time", "2 minutes")`: wait up to 2 minutes for late events. Spark
   guarantees that events up to 2 minutes late are counted, and uses the watermark to clean up old
   state. (It does not guarantee that *later* events are dropped; they are still kept in bronze.)
2. `dropDuplicatesWithinWatermark(["event_id"])`: remove repeated deliveries.
3. `groupBy(window(event_time, "5 minutes"), mode, route)`: average, p90 and max delay, number of
   vehicles and events per line and window.

The output mode is `update`: every micro-batch emits the windows that changed.

## 5. Write to Postgres safely (`foreach_batch`, `postgres_writer`)

Each micro-batch is small (one row per line per window), so it is collected and written with
`INSERT ... ON CONFLICT (window_start, mode, route) DO UPDATE`. This upsert is what makes results
exactly-once in effect: if Spark re-runs a batch after a crash, the same rows are overwritten, not
added. A second table, `pipeline_batches`, records each batch for the health panels.

One pitfall handled here: `collect()` returns *naive* datetimes in the Python process's local time
zone. `_utc()` converts them to aware UTC before they reach the `timestamptz` column.
`tests/test_streaming.py → test_window_timestamps_are_timezone_aware_utc` guards it.

## 6. Dashboards

- **Grafana** (`grafana/`): average delay per line, the worst lines in the last 15 minutes, events
  per minute, seconds since the last batch, event-time lag (from Postgres), plus message rate,
  dead-letter share and p95 event lag (from Prometheus).
- **Redpanda Console**: topics, consumer lag, and the dead-letter messages with their reasons.

## 7. How the tests prove the guarantees

The text file source in Spark gives the same `value` column as Kafka, so the tests run the exact
production functions on a local Spark session:

| Test | Proves |
| --- | --- |
| `test_duplicates_are_removed` | duplicate deliveries are counted once |
| `test_events_within_the_watermark_are_counted` | a 60-second-late event in a later batch still counts |
| `test_restart_from_checkpoint_gives_same_result` | crash + restart = same result as an uninterrupted run |
| `test_rerun_of_a_batch_does_not_double_count` | the Postgres upsert survives a replayed batch (real Postgres) |
| `test_router_sends_valid_to_main_and_invalid_to_dlq` | every message ends up in exactly one topic |

## 8. Report (`report.py`)

`python -m hsl_pipeline.report` runs the synthetic sample through the same code path (validation,
then a real streaming query over a file source) and writes `docs/results.md` and
`docs/img/delay_windows.png`.
