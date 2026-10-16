-- Serving tables for Grafana. The streaming job upserts into these.

CREATE TABLE IF NOT EXISTS line_delay_5min (
    window_start  timestamptz NOT NULL,
    window_end    timestamptz NOT NULL,
    mode          text        NOT NULL,
    route         text        NOT NULL,
    line          text,
    avg_delay_s   double precision,
    p90_delay_s   integer,
    max_delay_s   integer,
    vehicles      bigint,
    events        bigint,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, mode, route)
);

CREATE INDEX IF NOT EXISTS line_delay_5min_route_idx ON line_delay_5min (route, window_start DESC);

-- One row per Spark micro-batch: used for the pipeline health panels.
CREATE TABLE IF NOT EXISTS pipeline_batches (
    batch_id        bigint PRIMARY KEY,
    rows_written    integer NOT NULL,
    max_event_time  timestamptz,
    processed_at    timestamptz NOT NULL DEFAULT now()
);
