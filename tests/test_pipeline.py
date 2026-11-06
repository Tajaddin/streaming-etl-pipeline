"""End-to-end pipeline tests + crash-resume."""

from __future__ import annotations

from pathlib import Path

import pytest

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


def test_restart_with_nothing_to_consume_keeps_watermark_floor(tmp_path):
    bus = InMemoryBus()
    db = tmp_path / "p.duckdb"
    ck_path = tmp_path / "ck.sqlite"
    _publish(bus, [("a", 100, 1), ("a", 1500, 9)])
    for _ in range(2):  # the second run finds the bus empty
        ck = Checkpointer(ck_path)
        with DuckDBSink(db) as sink:
            agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
            Pipeline(bus, agg, sink, checkpointer=ck).run_until_drained()
        ck.close()

    ck = Checkpointer(ck_path)
    with DuckDBSink(db) as sink:
        agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
        _publish(bus, [("a", 50, 1)])  # window [0,1000) closed in the first run
        stats = Pipeline(bus, agg, sink, checkpointer=ck).run_until_drained()
        assert stats.late_events == 1
    ck.close()


class _LogBus:
    """Kafka-like bus: records stay in the log, and a restarted consumer
    resumes after the last committed offset of each partition."""

    def __init__(self, num_partitions: int = 1) -> None:
        self._log: dict[int, list[Event]] = {p: [] for p in range(num_partitions)}
        self._pos = {p: 0 for p in range(num_partitions)}
        self.committed = {p: -1 for p in range(num_partitions)}

    def produce(self, event: Event) -> None:
        q = self._log[event.partition]
        event.offset = len(q)
        q.append(event)

    def consume(self, batch_size: int = 100, timeout_ms: int = 0) -> list[Event]:
        out: list[Event] = []
        while len(out) < batch_size:
            pulled = 0
            for p, q in self._log.items():
                if self._pos[p] < len(q) and len(out) < batch_size:
                    out.append(q[self._pos[p]])
                    self._pos[p] += 1
                    pulled += 1
            if pulled == 0:
                break
        return out

    def commit(self, offsets: dict[int, int]) -> None:
        for p, off in offsets.items():
            self.committed[p] = max(self.committed[p], off)

    def restart_consumer(self) -> None:
        self._pos = {p: off + 1 for p, off in self.committed.items()}


# (key, partition, event_time_ms, value). Out of order within each partition,
# one event per partition lands in an already-closed window.
_STREAM = [
    ("a", 0, 100, 1),
    ("b", 1, 150, 2),
    ("a", 0, 900, 3),
    ("b", 1, 400, 4),
    ("a", 0, 1300, 5),
    ("b", 1, 1100, 6),
    ("a", 0, 1250, 7),
    ("b", 1, 2400, 8),
    ("a", 0, 2600, 9),
    ("b", 1, 300, 10),
    ("a", 0, 3700, 11),
    ("b", 1, 3500, 12),
    ("a", 0, 2900, 13),
    ("b", 1, 4800, 14),
]


def _log_bus_with_stream() -> _LogBus:
    bus = _LogBus(num_partitions=2)
    for k, p, ts, v in _STREAM:
        bus.produce(Event(key=k, event_time_ms=ts, payload={"v": v}, partition=p))
    return bus


def _agg() -> TumblingAggregator:
    return TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=200)


def _uninterrupted(tmp_path: Path) -> tuple[list[tuple], set[tuple]]:
    with DuckDBSink(tmp_path / "ref.duckdb") as sink:
        Pipeline(_log_bus_with_stream(), _agg(), sink, batch_size=2).run_until_drained()
        return sink.fetch_windows(), set(sink.fetch_late())


def test_crash_before_drain_does_not_lose_open_windows(tmp_path):
    expected_windows, _ = _uninterrupted(tmp_path)
    bus = _log_bus_with_stream()
    db = tmp_path / "p.duckdb"
    ck_path = tmp_path / "ck.sqlite"

    ck = Checkpointer(ck_path)
    with DuckDBSink(db) as sink:
        pipe = Pipeline(bus, _agg(), sink, checkpointer=ck, batch_size=2, commit_every_batches=1)
        pipe.step()  # offsets 0 of both partitions are committed; their windows are still open
    ck.close()  # crash: the in-memory windows are gone

    bus.restart_consumer()
    ck = Checkpointer(ck_path)
    with DuckDBSink(db) as sink:
        Pipeline(bus, _agg(), sink, checkpointer=ck, batch_size=2).run_until_drained()
        assert sink.fetch_windows() == expected_windows
    ck.close()


@pytest.mark.parametrize("crash_after_batches", range(1, 8))
def test_restart_after_crash_matches_uninterrupted_run(tmp_path, crash_after_batches):
    expected_windows, expected_late = _uninterrupted(tmp_path)
    bus = _log_bus_with_stream()
    db = tmp_path / "p.duckdb"
    ck_path = tmp_path / "ck.sqlite"

    ck = Checkpointer(ck_path)
    with DuckDBSink(db) as sink:
        # commit_every_batches=2 leaves batches processed but not checkpointed.
        pipe = Pipeline(bus, _agg(), sink, checkpointer=ck, batch_size=2, commit_every_batches=2)
        for _ in range(crash_after_batches):
            pipe.step()
    ck.close()

    bus.restart_consumer()
    ck = Checkpointer(ck_path)
    with DuckDBSink(db) as sink:
        Pipeline(bus, _agg(), sink, checkpointer=ck, batch_size=2).run_until_drained()
        assert sink.fetch_windows() == expected_windows
        assert set(sink.fetch_late()) == expected_late
    ck.close()


def test_redelivered_events_already_checkpointed_are_skipped(tmp_path):
    class CommitFailsOnce(_LogBus):
        failed = False

        def commit(self, offsets):
            if not self.failed:
                self.failed = True
                raise RuntimeError("broker unavailable")
            super().commit(offsets)

    expected_windows, _ = _uninterrupted(tmp_path)
    bus = CommitFailsOnce(num_partitions=2)
    for k, p, ts, v in _STREAM:
        bus.produce(Event(key=k, event_time_ms=ts, payload={"v": v}, partition=p))
    db = tmp_path / "p.duckdb"
    ck_path = tmp_path / "ck.sqlite"

    ck = Checkpointer(ck_path)
    with DuckDBSink(db) as sink:
        pipe = Pipeline(bus, _agg(), sink, checkpointer=ck, batch_size=4, commit_every_batches=1)
        with pytest.raises(RuntimeError):
            pipe.step()  # checkpoint written, broker commit lost
    ck.close()

    bus.restart_consumer()  # redelivers offsets the checkpoint already holds
    ck = Checkpointer(ck_path)
    with DuckDBSink(db) as sink:
        Pipeline(bus, _agg(), sink, checkpointer=ck, batch_size=4).run_until_drained()
        assert sink.fetch_windows() == expected_windows
    ck.close()

