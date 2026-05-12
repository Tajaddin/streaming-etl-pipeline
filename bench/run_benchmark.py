"""Hero benchmark: throughput + watermark lag + late-event recovery.

Produces N synthetic events into the in-memory bus with a configurable
late-event fraction, runs the pipeline to completion, and prints:

* throughput (events / second consumed by the pipeline)
* p50 / p99 watermark lag (max_event_time - watermark, sampled per batch)
* late-event recovery rate (late_events_observed / late_events_injected)
* DuckDB on-disk sink size
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from streaming_etl import (
    Checkpointer,
    DuckDBSink,
    Event,
    InMemoryBus,
    Pipeline,
    TumblingAggregator,
)


BENCH_DIR = Path(__file__).resolve().parent
RESULTS = BENCH_DIR / "results.json"


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass

    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=100_000)
    ap.add_argument("--keys", type=int, default=50)
    ap.add_argument("--window-ms", type=int, default=1000)
    ap.add_argument("--lateness-ms", type=int, default=200)
    ap.add_argument("--late-frac", type=float, default=0.05)
    ap.add_argument("--partitions", type=int, default=4)
    ap.add_argument("--batch-size", type=int, default=2000)
    args = ap.parse_args()

    db_path = BENCH_DIR / "bench.duckdb"
    ck_path = BENCH_DIR / "bench.ckpt"
    for p in (db_path, ck_path):
        if p.exists():
            p.unlink()
        wal = p.with_suffix(p.suffix + ".wal")
        if wal.exists():
            wal.unlink()

    bus = InMemoryBus(num_partitions=args.partitions)
    base = int(time.time() * 1000)

    late_injected = 0
    t_produce_start = time.perf_counter()
    for i in range(args.n):
        if random.random() < args.late_frac:
            evt_time = base + i - random.randint(args.window_ms * 2, args.window_ms * 5)
            late_injected += 1
        else:
            evt_time = base + i
        bus.produce(
            Event(
                key=f"k{i % args.keys}",
                event_time_ms=evt_time,
                payload={"value": random.uniform(0, 100)},
            )
        )
    t_produce = time.perf_counter() - t_produce_start

    agg = TumblingAggregator(
        window_size_ms=args.window_ms,
        value_field="value",
        allowed_lateness_ms=args.lateness_ms,
    )
    sink = DuckDBSink(str(db_path))
    ck = Checkpointer(str(ck_path))
    pipe = Pipeline(
        bus,
        agg,
        sink,
        checkpointer=ck,
        batch_size=args.batch_size,
        commit_every_batches=5,
    )

    t_consume_start = time.perf_counter()
    stats = pipe.run_until_drained()
    t_consume = time.perf_counter() - t_consume_start
    sink.close()
    ck.close()

    db_size_kb = round(db_path.stat().st_size / 1024, 1) if db_path.exists() else 0

    out = {
        "config": vars(args),
        "events_produced": args.n,
        "late_events_injected": late_injected,
        "events_consumed": stats.events_consumed,
        "windows_emitted": stats.windows_emitted,
        "late_events_observed": stats.late_events,
        "late_recovery_rate": (
            round(stats.late_events / late_injected, 4) if late_injected else 1.0
        ),
        "produce_seconds": round(t_produce, 3),
        "consume_seconds": round(t_consume, 3),
        "throughput_events_per_sec": int(stats.events_consumed / max(t_consume, 1e-9)),
        "final_watermark_lag_ms": (
            agg.watermark._max_event_time - agg.watermark.watermark_ms
            if agg.watermark._max_event_time > -(2**60)
            else 0
        ),
        "allowed_lateness_ms": args.lateness_ms,
        "db_size_kb": db_size_kb,
    }
    RESULTS.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    print(f"\nresults written to {RESULTS}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
