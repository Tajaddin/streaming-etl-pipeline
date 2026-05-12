"""Pipeline orchestrator.

A single ``Pipeline`` instance:

1. Pulls events from a :class:`MessageBus` in batches.
2. Routes each event through a :class:`TumblingAggregator`.
3. Drains finalised windows to a :class:`DuckDBSink`.
4. Routes late events to the same sink's ``late_events`` table.
5. Periodically commits offsets to the bus and persists a checkpoint.

The ``run_for`` loop is the workhorse; ``run_until_drained`` is the
benchmark-friendly variant that exits when the bus reports an empty
backlog and the aggregator has nothing left to flush.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Iterator

from streaming_etl.bus import MessageBus
from streaming_etl.checkpoint import Checkpointer
from streaming_etl.event import Event
from streaming_etl.sink import DuckDBSink
from streaming_etl.watermark import TumblingAggregator


@dataclass
class PipelineStats:
    events_consumed: int = 0
    windows_emitted: int = 0
    late_events: int = 0
    batches: int = 0
    started_at: float = field(default_factory=time.time)
    last_offsets: dict[int, int] = field(default_factory=dict)
    last_watermark_ms: int = -(2**63)

    @property
    def elapsed_seconds(self) -> float:
        return max(time.time() - self.started_at, 1e-9)

    @property
    def events_per_second(self) -> float:
        return self.events_consumed / self.elapsed_seconds


class Pipeline:
    def __init__(
        self,
        bus: MessageBus,
        aggregator: TumblingAggregator,
        sink: DuckDBSink,
        *,
        checkpointer: Checkpointer | None = None,
        batch_size: int = 500,
        commit_every_batches: int = 5,
        consume_timeout_ms: int = 50,
    ) -> None:
        self.bus = bus
        self.aggregator = aggregator
        self.sink = sink
        self.checkpointer = checkpointer
        self.batch_size = batch_size
        self.commit_every_batches = commit_every_batches
        self.consume_timeout_ms = consume_timeout_ms
        self.stats = PipelineStats()

        # On startup, restore watermark + offsets so we don't double-emit windows.
        if checkpointer:
            prior_wm = checkpointer.load_watermark()
            if prior_wm is not None:
                self.aggregator.watermark.set_floor(prior_wm)

    def _process_batch(self, batch: list[Event]) -> None:
        late_buf = []
        for evt in batch:
            late = self.aggregator.add(evt)
            if late:
                late_buf.extend(late)
            off = self.stats.last_offsets.get(evt.partition, -1)
            if evt.offset > off:
                self.stats.last_offsets[evt.partition] = evt.offset
        # Now flush any windows that the latest watermark allows us to close.
        finished = self.aggregator.flush_ready()
        if finished:
            self.sink.write_windows(finished)
            self.stats.windows_emitted += len(finished)
        if late_buf:
            self.sink.write_late(late_buf)
            self.stats.late_events += len(late_buf)
        self.stats.events_consumed += len(batch)
        self.stats.last_watermark_ms = self.aggregator.watermark.watermark_ms

    def _commit(self) -> None:
        if self.stats.last_offsets:
            self.bus.commit(self.stats.last_offsets)
        if self.checkpointer:
            if self.stats.last_offsets:
                self.checkpointer.save_offsets(self.stats.last_offsets)
            self.checkpointer.save_watermark(self.stats.last_watermark_ms)

    def step(self) -> int:
        """Pull one batch, process it, return events consumed."""
        batch = self.bus.consume(
            batch_size=self.batch_size, timeout_ms=self.consume_timeout_ms
        )
        if batch:
            self._process_batch(batch)
            self.stats.batches += 1
            if self.stats.batches % self.commit_every_batches == 0:
                self._commit()
        return len(batch)

    def run_until_drained(self, idle_polls_before_exit: int = 3) -> PipelineStats:
        idle = 0
        while True:
            n = self.step()
            if n == 0:
                idle += 1
                if idle >= idle_polls_before_exit:
                    break
            else:
                idle = 0
        final = self.aggregator.flush_all()
        if final:
            self.sink.write_windows(final)
            self.stats.windows_emitted += len(final)
        self._commit()
        return self.stats

    def run_for(self, seconds: float) -> PipelineStats:
        deadline = time.time() + seconds
        while time.time() < deadline:
            self.step()
        self._commit()
        return self.stats
