"""InMemoryBus unit tests."""

from __future__ import annotations

import threading
import time

from streaming_etl import Event, InMemoryBus


def test_produce_then_consume_returns_event():
    bus = InMemoryBus()
    bus.produce(Event(key="a", event_time_ms=1, payload={"v": 1}))
    out = bus.consume(batch_size=10, timeout_ms=10)
    assert len(out) == 1
    assert out[0].key == "a"
    assert out[0].payload == {"v": 1}


def test_consume_returns_empty_after_timeout():
    bus = InMemoryBus()
    t0 = time.time()
    out = bus.consume(batch_size=10, timeout_ms=20)
    elapsed = time.time() - t0
    assert out == []
    assert elapsed >= 0.018, f"timeout did not wait: {elapsed}s"


def test_offsets_increase_per_partition():
    bus = InMemoryBus(num_partitions=2)
    bus.produce(Event(key="a", event_time_ms=1))
    bus.produce(Event(key="a", event_time_ms=2))
    bus.produce(Event(key="b", event_time_ms=3))
    out = bus.consume(batch_size=10, timeout_ms=10)
    by_key = {e.key: [] for e in out}
    for e in out:
        by_key[e.key].append(e.offset)
    for offs in by_key.values():
        # offsets within a key must be strictly increasing
        assert offs == sorted(offs)
        assert len(set(offs)) == len(offs)


def test_same_key_lands_on_same_partition():
    bus = InMemoryBus(num_partitions=4)
    for i in range(10):
        bus.produce(Event(key="hot", event_time_ms=i))
    out = bus.consume(batch_size=100, timeout_ms=10)
    partitions = {e.partition for e in out}
    assert len(partitions) == 1, f"same key spread across partitions: {partitions}"


def test_commit_records_offset_per_partition():
    bus = InMemoryBus(num_partitions=2)
    bus.produce(Event(key="a", event_time_ms=1))
    bus.produce(Event(key="b", event_time_ms=2))
    _ = bus.consume(batch_size=10, timeout_ms=10)
    bus.commit({0: 5, 1: 7})
    stats = bus.stats()
    assert stats["committed"][0] == 5
    assert stats["committed"][1] == 7


def test_signal_wakes_a_blocked_consumer():
    bus = InMemoryBus()
    result: list = []

    def consumer():
        result.extend(bus.consume(batch_size=10, timeout_ms=2000))

    t = threading.Thread(target=consumer)
    t.start()
    time.sleep(0.05)
    bus.produce(Event(key="x", event_time_ms=1))
    t.join(timeout=1.0)
    assert not t.is_alive(), "consumer never woke up"
    assert len(result) == 1
