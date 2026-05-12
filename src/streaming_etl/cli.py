"""``streaming-etl`` CLI — runs a small demo with the in-memory bus."""

from __future__ import annotations

import random
import time
from pathlib import Path

import click

from streaming_etl import (
    Checkpointer,
    DuckDBSink,
    Event,
    InMemoryBus,
    Pipeline,
    TumblingAggregator,
)


@click.group()
def cli():
    """streaming-etl-pipeline demo + benchmark commands."""


@cli.command()
@click.option("--n", default=10_000, help="Number of events to produce.")
@click.option("--keys", default=10, help="Number of distinct keys.")
@click.option("--window-ms", default=1000, help="Tumbling window size in ms.")
@click.option("--lateness-ms", default=200, help="Allowed lateness in ms.")
@click.option("--late-frac", default=0.05, help="Fraction of events that are late.")
@click.option("--db", default="bench.duckdb", help="DuckDB output file.")
@click.option("--ckpt", default="bench.ckpt", help="Checkpoint SQLite file.")
def demo(n, keys, window_ms, lateness_ms, late_frac, db, ckpt):
    """Run the pipeline against a synthetic event stream."""
    bus = InMemoryBus(num_partitions=4)
    base = int(time.time() * 1000)
    for i in range(n):
        if random.random() < late_frac:
            evt_time = base + i - random.randint(window_ms, window_ms * 5)
        else:
            evt_time = base + i
        bus.produce(
            Event(
                key=f"k{i % keys}",
                event_time_ms=evt_time,
                payload={"value": random.uniform(0, 100)},
            )
        )

    Path(db).unlink(missing_ok=True)
    Path(ckpt).unlink(missing_ok=True)
    agg = TumblingAggregator(
        window_size_ms=window_ms,
        value_field="value",
        allowed_lateness_ms=lateness_ms,
    )
    with DuckDBSink(db) as sink:
        ck = Checkpointer(ckpt)
        pipe = Pipeline(bus, agg, sink, checkpointer=ck, batch_size=500)
        stats = pipe.run_until_drained()

    click.echo(
        f"events: {stats.events_consumed}  "
        f"windows: {stats.windows_emitted}  "
        f"late: {stats.late_events}  "
        f"throughput: {stats.events_per_second:,.0f} ev/s  "
        f"watermark_lag_ms: {base + n - stats.last_watermark_ms}"
    )


if __name__ == "__main__":
    cli()
