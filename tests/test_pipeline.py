"""End-to-end pipeline tests + crash-resume."""

from __future__ import annotations

from pathlib import Path

from streaming_etl import (
    Checkpointer,
    DuckDBSink,
    Event,
    InMemoryBus,
    Pipeline,
    TumblingAggregator,
)


def _publish(bus, items):
    for k, ts, v in items:
        bus.produce(Event(key=k, event_time_ms=ts, payload={"v": v}))


def test_end_to_end_one_window(tmp_path):
    bus = InMemoryBus()
    agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
    db = tmp_path / "p.duckdb"
    with DuckDBSink(db) as sink:
        pipe = Pipeline(bus, agg, sink, batch_size=100)
        _publish(
            bus,
            [
                ("a", 100, 1),
                ("a", 200, 2),
                ("a", 1500, 99),  # closes [0,1000)
            ],
        )
        stats = pipe.run_until_drained()
        assert stats.events_consumed == 3
        rows = sink.fetch_windows()
        # one closed window for "a" in [0,1000) + one for [1000,2000) on flush_all
        assert len(rows) == 2


def test_late_events_routed_to_late_table(tmp_path):
    bus = InMemoryBus()
    agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
    db = tmp_path / "p.duckdb"
    with DuckDBSink(db) as sink:
        pipe = Pipeline(bus, agg, sink, batch_size=100)
        _publish(
            bus,
            [
                ("k", 500, 1),
                ("k", 1500, 2),  # closes [0,1000)
                ("k", 2500, 3),  # closes [1000,2000)
                ("k", 600, 9),  # LATE
            ],
        )
        stats = pipe.run_until_drained()
        assert stats.late_events == 1
        assert sink.count_late() == 1


def test_idempotent_window_writes_on_replay(tmp_path):
    bus = InMemoryBus()
    agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
    db = tmp_path / "p.duckdb"
    with DuckDBSink(db) as sink:
        pipe1 = Pipeline(bus, agg, sink, batch_size=100)
        _publish(bus, [("a", 100, 1), ("a", 1500, 9)])
        pipe1.run_until_drained()
        count_after_first = sink.count_windows()

        # Replay the same events through a fresh aggregator. Window count
        # must stay constant because of the PRIMARY KEY upsert.
        agg2 = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
        pipe2 = Pipeline(bus, agg2, sink, batch_size=100)
        _publish(bus, [("a", 100, 1), ("a", 1500, 9)])
        pipe2.run_until_drained()
        assert sink.count_windows() == count_after_first


def test_checkpoint_roundtrips_offsets_and_watermark(tmp_path):
    ck = Checkpointer(tmp_path / "ck.sqlite")
    ck.save_offsets({0: 42, 1: 77})
    ck.save_watermark(123456)
    assert ck.load_offsets() == {0: 42, 1: 77}
    assert ck.load_watermark() == 123456


def test_pipeline_persists_checkpoint(tmp_path):
    bus = InMemoryBus()
    agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
    db = tmp_path / "p.duckdb"
    ck = Checkpointer(tmp_path / "ck.sqlite")
    with DuckDBSink(db) as sink:
        pipe = Pipeline(
            bus,
            agg,
            sink,
            checkpointer=ck,
            batch_size=2,
            commit_every_batches=1,
        )
        _publish(bus, [("a", i, 1) for i in range(10)])
        _publish(bus, [("a", 2000, 9)])
        pipe.run_until_drained()
        assert ck.load_watermark() == agg.watermark.watermark_ms
        assert ck.load_offsets()


def test_restart_does_not_double_emit_old_windows(tmp_path):
    bus = InMemoryBus()
    db = tmp_path / "p.duckdb"
    ck_path = tmp_path / "ck.sqlite"

    # Run 1: consume events from a 0..1000 window.
    agg1 = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
    ck1 = Checkpointer(ck_path)
    with DuckDBSink(db) as sink:
        pipe = Pipeline(bus, agg1, sink, checkpointer=ck1, batch_size=100)
        _publish(bus, [("a", 100, 1), ("a", 200, 2), ("a", 1500, 9)])
        pipe.run_until_drained()
    ck1.close()

    # Run 2: fresh aggregator, but checkpoint should seed the watermark so a
    # rogue late event for window [0,1000) is treated as late, not re-aggregated.
    agg2 = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
    ck2 = Checkpointer(ck_path)
    with DuckDBSink(db) as sink:
        pipe2 = Pipeline(bus, agg2, sink, checkpointer=ck2, batch_size=100)
        _publish(bus, [("a", 50, 1)])  # would fall in [0,1000)
        stats = pipe2.run_until_drained()
        # The event must be flagged late, not re-aggregated.
        assert stats.late_events == 1


def test_run_for_respects_timeout(tmp_path):
    import time

    bus = InMemoryBus()
    agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
    db = tmp_path / "p.duckdb"
    with DuckDBSink(db) as sink:
        pipe = Pipeline(bus, agg, sink, batch_size=100, consume_timeout_ms=20)
        t0 = time.time()
        pipe.run_for(0.1)
        elapsed = time.time() - t0
        assert 0.1 <= elapsed < 0.5, f"unexpected run time {elapsed}s"
