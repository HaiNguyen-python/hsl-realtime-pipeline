"""Integration test for the Postgres sink. Runs when POSTGRES_TEST_DSN is set
(CI starts a Postgres service; locally: docker compose up postgres)."""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

DSN = os.getenv("POSTGRES_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="POSTGRES_TEST_DSN not set")


@pytest.fixture()
def conn():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute(Path("sql/init.sql").read_text())
        c.execute("TRUNCATE line_delay_5min, pipeline_batches")
        yield c


def row(start, avg, events):
    return (start, start + timedelta(minutes=5), "tram", "1004", "4", avg, 90, 120, 3, events)


def test_rerun_of_a_batch_does_not_double_count(conn):
    from hsl_pipeline.streaming_job import postgres_writer

    write = postgres_writer(DSN)
    t0 = datetime(2026, 10, 15, 7, 0, tzinfo=UTC)
    write([row(t0, 30.0, 100)], batch_id=1)
    write([row(t0, 30.0, 100)], batch_id=1)  # same batch replayed after a crash
    write([row(t0, 45.0, 160)], batch_id=2)  # window updated by a later batch

    rows = conn.execute("SELECT avg_delay_s, events FROM line_delay_5min").fetchall()
    assert rows == [(45.0, 160)]
    batches = conn.execute("SELECT batch_id FROM pipeline_batches ORDER BY 1").fetchall()
    assert batches == [(1,), (2,)]
