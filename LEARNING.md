# Learning guide (for me, Hai)

Goal: explain this pipeline end to end, defend every design choice, and run the failure test live
in an interview if asked.

Start with [docs/how-it-works.md](docs/how-it-works.md) (the code walkthrough) and
[docs/decisions.md](docs/decisions.md) (the reasons). This file is the practice plan.

## 1. Run it (1–2 hours)

```bash
make install && make test          # 20 tests, local Spark
python -m hsl_pipeline.report      # results table + chart
make up                            # full stack with synthetic replay
```

Then:

1. Open Redpanda Console (:8080). Find the main topic and the dead-letter topic. Read three
   dead-letter messages and explain each `reason`.
2. Open Grafana (:3000). Which route has the jam? (Route 1004 in the synthetic data.)
3. Run `make crash-test`. Spark is killed for 60 seconds and restarted. Check that the dashboard
   catches up and that counts don't jump. Explain why (checkpoint + upsert).
4. Run `make up-live` for 30 minutes and record real numbers for the README.

## 2. Read the code in this order

| File | Understand | Ask yourself |
| --- | --- | --- |
| `schema.py` | Topic parsing, `dl` → `delay_s`, validators | Why validate in the producer instead of in Spark? |
| `router.py` | Dead-letter routing, keying, idempotent producer | What does `enable.idempotence` protect against? |
| `metrics.py` | Counter, Histogram, Gauge; what each one answers | Which metric tells you the feed has stopped? |
| `sample.py` | Duplicates, broken JSON, GPS errors, very late events | Which test covers each kind of mess? |
| `streaming_job.py` | `parse_events`, `delay_windows`, `foreachBatch` upsert | Why `update` output mode? Why upsert, not insert? |
| `tests/test_streaming.py` | File source as a stand-in for Kafka, checkpoint restart | How does the restart test prove exactly-once results? |
| `docker-compose.yml` | Services, health checks, profiles | What starts first and why? |

## 3. Concepts in my own words (fill in)

- **Event time vs processing time:** ...
- **Watermark:** ...  (Spark guarantees data within the delay is counted; older data *may* be dropped.)
- **At-least-once vs exactly-once:** ...
- **Idempotent sink:** ...
- **Dead-letter queue:** ...
- **Partition key and ordering:** ...
- **Prometheus metric types (counter, gauge, histogram):** ...
- **Naive vs aware datetimes:** ...  (why `_utc()` exists in `streaming_job.py`)

## 4. Interview questions this repo answers

1. *Batch vs streaming?* Freshness need (seconds vs hours), cost, complexity. Here: operations
   need minutes, so streaming; bronze Parquet still allows batch analysis later.
2. *How do you handle late or duplicate events?* Event-time windows + 2-minute watermark,
   dedup by `event_id`, every raw event kept in bronze.
3. *What happens if the consumer crashes?* Offsets in the checkpoint; re-run batches are safe
   because the sink upserts. Show the test.
4. *How do you deal with bad data?* Validate early, route to a DLQ with a reason, monitor DLQ rate.
5. *How would you scale it?* More Kafka partitions, more Spark executors, all modes instead of trams,
   a managed Kafka and Spark (e.g. Confluent + Databricks), Iceberg for bronze.

## 5. Make it mine (do at least three)

- [ ] Record one hour of the real feed (`hsl-producer --record ... --minutes 60`) and replay it.
- [ ] Add a Grafana panel: map of the slowest segments (lat/lon from bronze).
- [ ] Add the GTFS timetable join for per-stop delays.
- [ ] Rewrite `delay_windows` in Apache Flink (PyFlink) and compare: then "Flink" on my CV is true.
- [ ] Record a 60-second GIF of the dashboard for the README.

## 6. Honesty checklist before pushing

- [ ] I can explain every file and the watermark guarantee without notes.
- [ ] README numbers are from my own runs.
- [ ] Planned work is under "What I'd do next", not described as done.

Note: the Docker stack (`make up`) was not run in the environment where this repo was first built
(no access to container registries there). The Python and Spark code is tested; run `make up` once
yourself and fix anything in the compose file before you demo it.
