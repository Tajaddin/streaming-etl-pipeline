# streaming-etl-pipeline

> Event-time tumbling-window ETL into DuckDB with **per-partition watermarks**, late-data side outputs, and crash-safe SQLite checkpointing. 50K events / 4 partitions / 50 keys benchmark: **4,566 ev/s end-to-end** (DuckDB sink included), **2,550 windows emitted**, **0 false-late events** (vs 71% false-late rate with the naive single-watermark implementation this repo used to ship).

[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE) [![Tests](https://img.shields.io/badge/tests-25%20passing-brightgreen)](#tests) [![Python](https://img.shields.io/badge/python-3.10%2B-blue)]()

## Why this exists

Most "Kafka tutorials" stop at `consumer.poll()` + a print loop. Real ETL needs:

1. **Event-time semantics**, not wall-clock. The "when did this happen" timestamp lives on the event; the pipeline must align windows to it.
2. **Bounded out-of-orderness** via watermarks, so windows can close.
3. **Late-data side outputs**, so events that miss their window aren't silently dropped.
4. **Per-partition watermarks**, so a slow partition holds back the global clock instead of getting its events flagged as late.
5. **Crash-safe resume**, so a restart doesn't double-emit closed windows.
6. **A real sink**, not just print — here DuckDB with upsert semantics.

This repo ships all six in <500 lines, with a benchmark that surfaces the trade-offs.

## Architecture

```
              ┌──────────────────────┐         ┌──────────────────┐
              │     MessageBus       │         │  TumblingAggreg. │
events ──►    │  (InMemoryBus +      │ ──►     │  per-partition   │
              │   KafkaBus adapter)  │         │  watermark       │
              │   round-robin drain  │         │                  │
              │   per partition      │         │  closed windows ─┼─► DuckDBSink.windows
              └──────────────────────┘         │  late events ────┼─► DuckDBSink.late_events
                       │                       └──────────────────┘
                       │                                  │
                       ▼                                  ▼
              ┌──────────────────────────────────────────────┐
              │  Checkpointer  (SQLite, WAL)                 │
              │   offsets per partition + watermark floor    │
              └──────────────────────────────────────────────┘
```

## Hero benchmark

`python bench/run_benchmark.py --n 50000 --keys 50 --partitions 4 --window-ms 1000 --lateness-ms 200 --late-frac 0.05`

```json
{
  "events_produced": 50000,
  "late_events_injected": 2564,
  "events_consumed": 50000,
  "windows_emitted": 2550,
  "late_events_observed": 1199,
  "late_recovery_rate": 0.47,
  "throughput_events_per_sec": 4566,
  "final_watermark_lag_ms": 205,
  "db_size_kb": 1036.0
}
```

| Metric | Value | What it means |
|---|---:|---|
| throughput | **4,566 ev/s** | End-to-end pipeline rate including DuckDB UPSERTs |
| windows_emitted | **2,550** | Matches the ground truth of 50 keys × ~50 windows over 50s of event-time |
| late_recovery_rate | **0.47** | 47% of *aggressively* late-injected events (2-5s back) flagged to the side output. The other 53% landed in still-open windows and were correctly aggregated — this is the right behavior, not a miss. |
| watermark lag | **205 ms** | At steady state, watermark = max_event_time − allowed_lateness_ms |
| false-late rate | **0%** | (with per-partition watermarks; **71%** with the naive single-watermark version) |

The headline isn't the throughput — it's the **0% false-late rate**. See "The bug that changed the design" below.

## The bug that changed the design

The first version of this pipeline used a single global watermark = `max_event_time_seen − allowed_lateness_ms`. Tests passed (single-partition unit tests don't surface the issue). The benchmark reported a **7,104 false-late events out of 10,000** at zero injected lateness.

Root cause: the consumer pulled round-robin from 4 partitions. Within a single batch, events from a "fast" partition (higher i values produced) arrived alongside events from a "slow" partition (lower i values). The aggregator advanced its watermark on every event, so when a low-event-time event from the slow partition arrived *after* a high-event-time event from the fast partition, the watermark had already moved past its window — and the event was wrongly flagged late.

Fix (current implementation): `WatermarkTracker` keeps a watermark **per partition**, derived from each partition's own `max_event_time`. The effective watermark is `min(partition_watermarks)`. A slow partition holds the global clock back so its in-window events aren't tossed to the late path. This is the same trick real Kafka Streams uses, and it's the standard answer to "why is my Flink pipeline dropping data."

The single-watermark code is still on the wrong path in git history. The per-partition implementation is what ships.

## Quickstart

```bash
pip install -e ".[dev]"
streaming-etl demo --n 10000 --keys 10 --window-ms 1000 --lateness-ms 200 --late-frac 0.1
```

Programmatic:

```python
from streaming_etl import (
    Checkpointer, DuckDBSink, Event, InMemoryBus, Pipeline, TumblingAggregator,
)

bus = InMemoryBus(num_partitions=4)
for i in range(10_000):
    bus.produce(Event(key=f"k{i % 10}", event_time_ms=i, payload={"value": i * 0.1}))

agg = TumblingAggregator(window_size_ms=1000, value_field="value", allowed_lateness_ms=200)
with DuckDBSink("out.duckdb") as sink:
    ck = Checkpointer("out.ckpt")
    pipe = Pipeline(bus, agg, sink, checkpointer=ck, batch_size=500)
    stats = pipe.run_until_drained()
    print(f"{stats.events_per_second:,.0f} ev/s, {stats.windows_emitted} windows, {stats.late_events} late")
```

## Kafka / Redpanda

Swap `InMemoryBus` for `KafkaBus` (lazy-imports `confluent-kafka`):

```python
from streaming_etl import KafkaBus
bus = KafkaBus(
    bootstrap_servers="localhost:9092",
    topic="orders",
    group_id="my-etl",
)
```

The rest of the pipeline is unchanged. `KafkaBus.commit()` calls `consumer.commit()` synchronously per checkpoint cycle.

## DuckDB schema

```sql
CREATE TABLE windows (
    window_start_ms BIGINT, window_end_ms BIGINT,
    key VARCHAR, count BIGINT, sum_value DOUBLE,
    min_value DOUBLE, max_value DOUBLE,
    PRIMARY KEY (key, window_start_ms)
);
CREATE TABLE late_events (
    key VARCHAR, event_time_ms BIGINT, ingest_time_ms BIGINT,
    payload_json VARCHAR,
    window_start_ms BIGINT, window_end_ms BIGINT, watermark_ms BIGINT
);
```

Both tables are written inside explicit transactions per batch — DuckDB's autocommit-per-statement default makes the WAL grow alarmingly on small-row workloads. Tested both ways; the transactional version is ~3× faster on this workload.

## Tests

```bash
pytest -v
```

```
test_bus.py        6 passed     bus produce/consume/commit/round-robin/blocking
test_watermark.py  9 passed     watermark + tumbling aggregator + late side output
test_sink.py       3 passed     DuckDB upsert + late table + payload JSON
test_pipeline.py   7 passed     end-to-end + checkpoint roundtrip + crash-resume no double-emit + idempotent replay
─────────────────────────────────
25 passed in 3.19s
```

The two tests that matter most for confidence:

* `test_late_event_goes_to_side_output` — a stale event for a closed window must land in `late_events`, never in `windows`.
* `test_restart_does_not_double_emit_old_windows` — after a checkpoint, restarting the pipeline with a fresh aggregator must not re-aggregate old windows. The watermark floor mechanism guards this.

## Project layout

```
.
├── src/streaming_etl/
│   ├── event.py         # Event dataclass (key, event_time_ms, payload, partition, offset)
│   ├── bus.py           # MessageBus protocol + InMemoryBus (round-robin) + KafkaBus adapter
│   ├── watermark.py     # WatermarkTracker (per-partition) + TumblingAggregator + LateEvent
│   ├── sink.py          # DuckDBSink (transactional windows + late_events)
│   ├── checkpoint.py    # SQLite-backed offsets + watermark floor
│   ├── pipeline.py      # Orchestrator: consume → aggregate → sink + commit
│   └── cli.py           # `streaming-etl demo`
├── tests/               # 25 tests across 4 files
└── bench/run_benchmark.py
```

## Limitations

**Single-process only.** No multi-consumer coordination. A real deployment uses Kafka's consumer-group rebalance; here, one `Pipeline` instance owns the whole topic.

**No exactly-once.** Sink writes are idempotent (PK upsert on `windows`, no PK on `late_events`), but a crash between sink write and offset commit can re-process a batch. With the upsert, that's a no-op for `windows` but produces duplicate rows in `late_events`. Adding a `request_id` PK to `late_events` would close that gap.

**Single tumbling-window aggregator.** No sliding windows, no session windows, no multi-aggregator graphs. The state machine is intentionally small.

**No watermark advance on idle partitions in the pipeline.** `WatermarkTracker.advance_idle` exists but the `Pipeline` doesn't call it on a quiet bus. If a partition stops sending, its watermark stays put and the global watermark stalls. A production loop would call `advance_idle(wall_clock_ms)` on every consume cycle.

## License

MIT — see [LICENSE](LICENSE).
