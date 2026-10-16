"""Streaming tests with a local Spark session and a file source.

The text file source gives the same `value` column as Kafka, so the exact same
parse_events + delay_windows code runs here as in production.
"""

import json
import shutil
from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")

from hsl_pipeline.router import MessageRouter  # noqa: E402
from hsl_pipeline.sample import generate  # noqa: E402
from hsl_pipeline.streaming_job import delay_windows, foreach_batch, parse_events  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def valid_events(records):
    out = []
    router = MessageRouter(lambda t, k, v: out.append(v) if t == "main" else None, "main", "dlq")
    for r in records:
        router.handle(r["topic"], r["payload"])
    return out


def write_lines(path, lines):
    path.write_text("\n".join(lines) + "\n")


class UpsertSink:
    """Behaves like the Postgres sink: last write per window key wins."""

    def __init__(self):
        self.rows = {}
        self.batches = []

    def __call__(self, rows, batch_id):
        self.batches.append(batch_id)
        for r in rows:
            self.rows[(r[0], r[2], r[3])] = r


def run_stream(spark, src, checkpoint, sink):
    raw = spark.readStream.option("maxFilesPerTrigger", 1).text(str(src))
    query = (
        delay_windows(parse_events(raw), "2 minutes", "5 minutes")
        .writeStream.outputMode("update")
        .foreachBatch(foreach_batch(sink))
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


@pytest.fixture(scope="module")
def events():
    records = generate(minutes=12, vehicles_per_route=2, seed=3)
    # The generator ends with 20 very late events; keep them apart from the on-time stream.
    on_time, late = records[:-20], records[-20:]
    return valid_events(on_time), valid_events(late)


def total_events(sink):
    return sum(r[9] for r in sink.rows.values())


def test_duplicates_are_removed(spark, tmp_path, events):
    on_time, _ = events
    src = tmp_path / "src"
    src.mkdir()
    write_lines(src / "part-0001.json", on_time)
    sink = UpsertSink()
    run_stream(spark, src, tmp_path / "chk", sink)

    unique_ids = {json.loads(v)["event_id"] for v in on_time}
    assert len(on_time) > len(unique_ids), "sample should contain duplicate deliveries"
    assert total_events(sink) == len(unique_ids)


def test_events_within_the_watermark_are_counted(spark, tmp_path, events):
    """An event 60 s late (watermark is 2 min) arriving in a later batch must be counted."""
    on_time, _ = events
    last = max(datetime.fromisoformat(json.loads(v)["event_time"]) for v in on_time)
    late_event = json.loads(on_time[-1])
    late_event.update(
        event_id="late:1",
        event_time=(last - timedelta(seconds=60)).isoformat().replace("+00:00", "Z"),
        route="1001",
    )
    src = tmp_path / "src"
    src.mkdir()
    write_lines(src / "part-0001.json", on_time)
    write_lines(src / "part-0002.json", [json.dumps(late_event)])
    sink = UpsertSink()
    run_stream(spark, src, tmp_path / "chk", sink)

    unique_ids = {json.loads(v)["event_id"] for v in on_time}
    assert total_events(sink) == len(unique_ids) + 1


def test_jam_shows_up_in_delay_windows(spark, tmp_path, events):
    on_time, _ = events
    src = tmp_path / "src"
    src.mkdir()
    write_lines(src / "part-0001.json", on_time)
    sink = UpsertSink()
    run_stream(spark, src, tmp_path / "chk", sink)
    # Route 1004 builds a jam in the second half: its last window is later than its first.
    jam = sorted((r for r in sink.rows.values() if r[3] == "1004"), key=lambda r: r[0])
    assert jam[-1][5] > jam[0][5]


def test_restart_from_checkpoint_gives_same_result(spark, tmp_path, events):
    on_time, late = events
    half = len(on_time) // 2

    # Uninterrupted run over both files.
    full_src = tmp_path / "full"
    full_src.mkdir()
    write_lines(full_src / "part-0001.json", on_time[:half])
    write_lines(full_src / "part-0002.json", on_time[half:])
    reference = UpsertSink()
    run_stream(spark, full_src, tmp_path / "chk_full", reference)

    # "Crash" after the first file, then restart with the same checkpoint.
    src = tmp_path / "crash"
    src.mkdir()
    write_lines(src / "part-0001.json", on_time[:half])
    sink = UpsertSink()
    run_stream(spark, src, tmp_path / "chk_crash", sink)
    shutil.copy(full_src / "part-0002.json", src / "part-0002.json")
    run_stream(spark, src, tmp_path / "chk_crash", sink)

    assert sink.rows == reference.rows
    assert isinstance(next(iter(sink.rows.values()))[0], datetime)


def test_window_timestamps_are_timezone_aware_utc(spark, tmp_path, events):
    """Guards the collect() pitfall: rows handed to the sink must be aware UTC datetimes."""
    on_time, _ = events
    src = tmp_path / "src"
    src.mkdir()
    write_lines(src / "part-0001.json", on_time[:200])
    sink = UpsertSink()
    run_stream(spark, src, tmp_path / "chk", sink)
    first = min(r[0] for r in sink.rows.values())
    assert first.tzinfo is not None and first.utcoffset().total_seconds() == 0
    assert first.isoformat().startswith("2026-10-15T07:00")  # the sample starts 07:00 UTC
