"""Message-bus abstraction.

Real deployments wire :class:`KafkaBus` against a Kafka or Redpanda cluster.
Tests and the local benchmark use :class:`InMemoryBus`, which exposes the
same surface (produce / consume / commit) without an external broker.

The contract is intentionally minimal:

* ``produce(event)`` appends a record to the topic.
* ``consume(batch_size, timeout_ms)`` returns up to ``batch_size`` records,
  blocking up to ``timeout_ms`` for the first arrival.
* ``commit(offsets)`` durably stores per-partition offsets the consumer is
  done with — the bus may garbage-collect records up to that point.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Any, Iterator, Protocol

from streaming_etl.event import Event


class MessageBus(Protocol):
    def produce(self, event: Event) -> None: ...
    def consume(self, batch_size: int = 100, timeout_ms: int = 100) -> list[Event]: ...
    def commit(self, offsets: dict[int, int]) -> None: ...


class InMemoryBus:
    """Single-process bus that mirrors Kafka's offset / partition semantics."""

    def __init__(self, num_partitions: int = 1, retain_after_commit: bool = False) -> None:
        self.num_partitions = num_partitions
        self.retain_after_commit = retain_after_commit
        self._partitions: dict[int, deque[Event]] = {
            p: deque() for p in range(num_partitions)
        }
        self._offsets: dict[int, int] = {p: 0 for p in range(num_partitions)}
        self._committed: dict[int, int] = {p: 0 for p in range(num_partitions)}
        self._lock = threading.Lock()
        self._signal = threading.Event()

    def _partition_for(self, key: str) -> int:
        # Stable hash so the same key always lands on the same partition.
        return (hash(key) & 0x7FFFFFFF) % self.num_partitions

    def produce(self, event: Event) -> None:
        with self._lock:
            p = self._partition_for(event.key)
            event.partition = p
            event.offset = self._offsets[p]
            event.ingest_time_ms = event.ingest_time_ms or int(time.time() * 1000)
            self._partitions[p].append(event)
            self._offsets[p] += 1
            self._signal.set()

    def consume(self, batch_size: int = 100, timeout_ms: int = 100) -> list[Event]:
        deadline = time.time() + timeout_ms / 1000.0
        out: list[Event] = []
        while True:
            with self._lock:
                # True round-robin: one event per partition per pass. This keeps
                # per-partition event-time progress balanced; if we drained one
                # partition fully first, the watermark would zoom to its max
                # event_time and every subsequent partition's events would look
                # late.
                while len(out) < batch_size:
                    pulled = 0
                    for p in range(self.num_partitions):
                        q = self._partitions[p]
                        if q and len(out) < batch_size:
                            out.append(q.popleft())
                            pulled += 1
                    if pulled == 0:
                        break
                if out:
                    self._signal.clear()
                    return out
            remaining = deadline - time.time()
            if remaining <= 0:
                return out
            self._signal.wait(timeout=remaining)

    def commit(self, offsets: dict[int, int]) -> None:
        with self._lock:
            for p, off in offsets.items():
                if off > self._committed.get(p, -1):
                    self._committed[p] = off
            if not self.retain_after_commit:
                # Records produced before commit are already drained by consume();
                # this is a no-op for the in-memory bus. Kept for API parity.
                pass

    def stats(self) -> dict[str, Any]:
        with self._lock:
            backlog = {p: len(q) for p, q in self._partitions.items()}
            return {
                "backlog_per_partition": backlog,
                "total_backlog": sum(backlog.values()),
                "next_offset": dict(self._offsets),
                "committed": dict(self._committed),
            }


class KafkaBus:
    """Thin adapter around ``confluent_kafka.Producer`` / ``Consumer``.

    Imported lazily so the package works without confluent-kafka installed.
    Used in production; tests use :class:`InMemoryBus`.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        *,
        consumer_config: dict | None = None,
        producer_config: dict | None = None,
    ) -> None:
        from confluent_kafka import Consumer, Producer  # noqa: F401

        self.topic = topic
        prod_conf = {"bootstrap.servers": bootstrap_servers}
        prod_conf.update(producer_config or {})
        cons_conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
        cons_conf.update(consumer_config or {})
        self._producer = Producer(prod_conf)
        self._consumer = Consumer(cons_conf)
        self._consumer.subscribe([topic])

    def produce(self, event: Event) -> None:
        import json

        body = json.dumps(
            {
                "key": event.key,
                "event_time_ms": event.event_time_ms,
                "payload": event.payload,
            }
        ).encode("utf-8")
        self._producer.produce(self.topic, value=body, key=event.key.encode("utf-8"))
        self._producer.poll(0)

    def consume(self, batch_size: int = 100, timeout_ms: int = 100) -> list[Event]:
        import json

        out: list[Event] = []
        deadline = time.time() + timeout_ms / 1000.0
        while len(out) < batch_size:
            remaining = max(0.0, deadline - time.time())
            msg = self._consumer.poll(timeout=remaining)
            if msg is None:
                break
            if msg.error():
                continue
            raw = json.loads(msg.value().decode("utf-8"))
            out.append(
                Event(
                    key=raw["key"],
                    event_time_ms=int(raw["event_time_ms"]),
                    payload=raw.get("payload", {}),
                    ingest_time_ms=int(time.time() * 1000),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
            )
        return out

    def commit(self, offsets: dict[int, int]) -> None:
        from confluent_kafka import TopicPartition

        tps = [TopicPartition(self.topic, p, off + 1) for p, off in offsets.items()]
        self._consumer.commit(offsets=tps, asynchronous=False)
