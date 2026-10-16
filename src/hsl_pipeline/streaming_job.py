"""Spark Structured Streaming job.

Kafka (validated events) --> bronze Parquet (all events, append, exactly-once file sink)
                         \\-> 5-minute delay windows per line --> Postgres (idempotent upsert)

Correctness under failure:
- Spark tracks Kafka offsets in the checkpoint, so after a crash it resumes where it stopped.
- A micro-batch may be re-run after a crash; the Postgres sink upserts on the window key,
  so a re-run overwrites rows instead of double counting.
- Duplicate deliveries are removed with ``dropDuplicatesWithinWatermark`` on ``event_id``.
- The watermark bounds state: events up to ``watermark`` late are always counted; once the
  watermark passes a window's end, its state is cleaned up, and much later events may be
  ignored (Spark guarantees the first part, not the second). All events still land in bronze.

Run (inside the Spark container, see docker-compose.yml):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\
        jobs/run_streaming.py
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import Settings, get_settings

log = logging.getLogger("hsl_pipeline.streaming")

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("mode", T.StringType()),
        T.StructField("operator", T.IntegerType()),
        T.StructField("vehicle", T.IntegerType()),
        T.StructField("route", T.StringType()),
        T.StructField("line", T.StringType()),
        T.StructField("direction", T.IntegerType()),
        T.StructField("event_time", T.TimestampType()),
        T.StructField("delay_s", T.IntegerType()),
        T.StructField("speed_ms", T.DoubleType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
    ]
)

UPSERT_SQL = """
INSERT INTO line_delay_5min
    (window_start, window_end, mode, route, line, avg_delay_s, p90_delay_s, max_delay_s,
     vehicles, events, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (window_start, mode, route) DO UPDATE SET
    window_end = EXCLUDED.window_end, line = EXCLUDED.line,
    avg_delay_s = EXCLUDED.avg_delay_s, p90_delay_s = EXCLUDED.p90_delay_s,
    max_delay_s = EXCLUDED.max_delay_s, vehicles = EXCLUDED.vehicles,
    events = EXCLUDED.events, updated_at = now()
"""

HEALTH_SQL = """
INSERT INTO pipeline_batches (batch_id, rows_written, max_event_time, processed_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (batch_id) DO UPDATE SET
    rows_written = EXCLUDED.rows_written, max_event_time = EXCLUDED.max_event_time,
    processed_at = now()
"""


def parse_events(raw: DataFrame) -> DataFrame:
    """Kafka/text rows with a string ``value`` column -> typed event columns."""
    return (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .where(F.col("event_id").isNotNull() & F.col("event_time").isNotNull())
    )


def delay_windows(events: DataFrame, watermark: str, window: str) -> DataFrame:
    """Deduplicate within the watermark, then aggregate delays per line and window."""
    return (
        events.withWatermark("event_time", watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
        .groupBy(F.window("event_time", window).alias("w"), "mode", "route")
        .agg(
            F.first("line", ignorenulls=True).alias("line"),
            F.round(F.avg("delay_s"), 1).alias("avg_delay_s"),
            F.percentile_approx("delay_s", 0.9).alias("p90_delay_s"),
            F.max("delay_s").alias("max_delay_s"),
            F.approx_count_distinct("vehicle").alias("vehicles"),
            F.count("*").alias("events"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "mode",
            "route",
            "line",
            "avg_delay_s",
            "p90_delay_s",
            "max_delay_s",
            "vehicles",
            "events",
        )
    )


Writer = Callable[[list[tuple], int], None]


def postgres_writer(dsn: str) -> Writer:
    def write(rows: list[tuple], batch_id: int) -> None:
        import psycopg

        max_event = max((r[1] for r in rows), default=None)
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            if rows:
                cur.executemany(UPSERT_SQL, rows)
            cur.execute(HEALTH_SQL, (batch_id, len(rows), max_event))
        log.info("batch %d: upserted %d window rows", batch_id, len(rows))

    return write


def _utc(ts: datetime) -> datetime:
    return ts.astimezone(UTC) if ts.tzinfo is None else ts


def foreach_batch(writer: Writer) -> Callable[[DataFrame, int], None]:
    """Adapter: collect the (small) aggregated batch and hand it to the writer.

    Timestamp pitfall: ``collect()`` returns *naive* datetimes in the Python process's
    local time zone, whatever ``spark.sql.session.timeZone`` says. Written as-is into a
    ``timestamptz`` column they would be shifted by the server's offset. ``astimezone``
    on a naive datetime interprets it as local time, so this makes them correct UTC.
    """

    def handle(batch: DataFrame, batch_id: int) -> None:
        rows = [
            (
                _utc(r.window_start),
                _utc(r.window_end),
                r.mode,
                r.route,
                r.line,
                r.avg_delay_s,
                r.p90_delay_s,
                r.max_delay_s,
                r.vehicles,
                r.events,
            )
            for r in batch.collect()
        ]
        writer(rows, batch_id)

    return handle


def build_spark(app_name: str = "hsl-realtime-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run(settings: Settings | None = None) -> None:
    s = settings or get_settings()
    spark = build_spark()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap)
        .option("subscribe", s.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200_000)
        .load()
    )
    events = parse_events(raw)

    bronze = (
        events.withColumn("event_date", F.to_date("event_time"))
        .writeStream.format("parquet")
        .option("path", s.bronze_path)
        .option("checkpointLocation", f"{s.checkpoint_dir}/bronze")
        .partitionBy("event_date", "mode")
        .trigger(processingTime=s.trigger)
        .start()
    )
    windows = (
        delay_windows(events, s.watermark, s.window)
        .writeStream.outputMode("update")
        .foreachBatch(foreach_batch(postgres_writer(s.postgres_dsn)))
        .option("checkpointLocation", f"{s.checkpoint_dir}/windows")
        .trigger(processingTime=s.trigger)
        .start()
    )
    log.info("streams started: bronze=%s windows=%s", bronze.id, windows.id)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
