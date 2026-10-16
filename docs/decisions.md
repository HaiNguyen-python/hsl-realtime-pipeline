# Design decisions

## 1. Validate in the producer, not in Spark

- **Decision:** parse and validate each message before it reaches Kafka; bad ones go to a
  dead-letter topic with the reason.
- **Why:** consumers can trust the main topic's schema; bad data is visible and countable instead
  of silently dropped.
- **Cost:** the producer does more work (still about 34,000 messages/s on one core here, with
  metrics on; measure the live feed rate with `hsl-producer` before scaling decisions).

## 2. Kafka API with Redpanda locally

- **Decision:** Redpanda in Docker Compose.
- **Why:** Kafka-compatible, one container, no ZooKeeper; the same client code works with Apache
  Kafka, Confluent Cloud or Amazon MSK.

## 3. Key by vehicle

- **Decision:** the Kafka key is `operator:vehicle`.
- **Why:** per-vehicle order is preserved within a partition.
- **Cost:** a very busy vehicle can't be spread over partitions (not a problem at this scale).

## 4. Event time with a 2-minute watermark

- **Decision:** windows on the GPS timestamp; watermark 2 minutes.
- **Why:** arrival order is not event order. Two minutes covers normal network delay while keeping
  state small.
- **Cost:** results for a window are final only about 2 minutes after it ends. Spark does not
  promise to drop events later than that, so bronze keeps everything for batch correction.

## 5. Exactly-once results from three parts

- **Decision:** replayable source (Kafka offsets in the checkpoint) + exactly-once file sink for
  bronze + idempotent upsert for Postgres.
- **Why:** true end-to-end exactly-once delivery to an external database is expensive; an
  idempotent sink gives the same result for this use case.
- **Proof:** the restart test and the replayed-batch Postgres test.

## 6. Spark Structured Streaming (not Flink) for now

- **Decision:** Spark 3.5.
- **Why:** one engine for batch and streaming, widely used in data engineering teams, easy
  local testing with the file source.
- **Alternative:** Flink has lower latency and richer event-time features; rewriting
  `delay_windows` in PyFlink is a planned comparison.

## 7. Trams only by default

- **Decision:** subscribe to `/vp/tram/#` by default.
- **Why:** a few hundred messages per second is easy on a laptop; set `MQTT_TOPIC` for all modes.

## 8. Synthetic sample with realistic mess

- **Decision:** `sample.py` creates duplicates, broken JSON, GPS errors, very late events and a
  traffic jam on one line.
- **Why:** every guarantee can be tested offline and in CI, without depending on the live feed.
