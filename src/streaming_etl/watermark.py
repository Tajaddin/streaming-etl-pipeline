"""Watermarks + event-time tumbling windows with late-data side outputs.

The watermark is a monotonic clock the pipeline derives from observed event
times: ``watermark = max_event_time - allowed_lateness_ms``. Windows whose
right edge falls below the watermark are considered finalised and can be
flushed downstream.

Events arriving with ``event_time_ms < watermark`` are LATE. They are not
discarded; they go to a side output (:class:`LateEvent`) so a downstream
job can decide how to reconcile (e.g. write to a "late" partition that
gets reprocessed nightly).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

from streaming_etl.event import Event


@dataclass(slots=True)
class WindowOutput:
    """One closed window."""

    window_start_ms: int
    window_end_ms: int  # exclusive
    key: str
    count: int
    sum_value: float
    min_value: float
    max_value: float


@dataclass(slots=True)
class LateEvent:
    """An event that arrived after its window had already closed."""

    event: Event
    window_start_ms: int
    window_end_ms: int
    watermark_ms: int


class WatermarkTracker:
    """Per-partition watermarks; effective watermark = min across partitions.

    For each partition, ``partition_watermark = max_event_time_in_partition - allowed_lateness_ms``.
    The pipeline's effective watermark is the **minimum** across all partitions
    we've observed at least one event from. This matches real Kafka consumer
    semantics: a slow partition holds back the watermark so events from
    elsewhere on the bus aren't falsely flagged late.

    A partition can be advanced from wall-clock via :meth:`advance_idle` to
    stop a quiet partition from stalling the global watermark.
    """

    def __init__(self, allowed_lateness_ms: int) -> None:
        self.allowed_lateness_ms = allowed_lateness_ms
        self._max_per_partition: dict[int, int] = {}
        self._watermark_per_partition: dict[int, int] = {}
        # restored-from-checkpoint floor — the effective watermark is never
        # below this value (used so restart doesn't re-open closed windows).
        self._floor: int = -(2**63)
        # cached effective watermark; recomputed lazily
        self._cached_effective: int = -(2**63)
        self._dirty: bool = True

    @property
    def _max_event_time(self) -> int:
        """Largest event_time we've ever observed across any partition."""
        if not self._max_per_partition:
            return -(2**63)
        return max(self._max_per_partition.values())

    @property
    def watermark_ms(self) -> int:
        if self._dirty:
            if self._watermark_per_partition:
                self._cached_effective = max(
                    self._floor, min(self._watermark_per_partition.values())
                )
            else:
                self._cached_effective = self._floor
            self._dirty = False
        return self._cached_effective

    def set_floor(self, value_ms: int) -> None:
        if value_ms > self._floor:
            self._floor = value_ms
            self._dirty = True

    def observe(self, event_time_ms: int, partition: int = 0) -> None:
        prev = self._max_per_partition.get(partition, -(2**63))
        if event_time_ms > prev:
            self._max_per_partition[partition] = event_time_ms
            candidate = event_time_ms - self.allowed_lateness_ms
            if candidate > self._watermark_per_partition.get(partition, -(2**63)):
                self._watermark_per_partition[partition] = candidate
                self._dirty = True

    def advance_idle(self, current_ms: int, partition: int | None = None) -> None:
        """Advance one partition's watermark from wall-clock. If ``partition``
        is None, advance every known partition."""
        partitions = [partition] if partition is not None else list(
            self._watermark_per_partition.keys()
        )
        candidate = current_ms - self.allowed_lateness_ms
        for p in partitions:
            if candidate > self._watermark_per_partition.get(p, -(2**63)):
                self._watermark_per_partition[p] = candidate
                self._max_per_partition[p] = max(
                    self._max_per_partition.get(p, -(2**63)), candidate
                )
                self._dirty = True


_AggregatorState = dict[tuple[str, int], dict[str, float]]


class TumblingAggregator:
    """Event-time tumbling windows on a single numeric field.

    Each (key, window_start) pair accumulates ``count``, ``sum``, ``min``,
    ``max`` of a configurable payload field. When the watermark advances
    past a window's end, that window is emitted as :class:`WindowOutput`.

    Events that arrive after their window has already been closed are
    emitted as :class:`LateEvent` instead.
    """

    def __init__(
        self,
        *,
        window_size_ms: int,
        value_field: str,
        allowed_lateness_ms: int = 0,
    ) -> None:
        if window_size_ms <= 0:
            raise ValueError("window_size_ms must be > 0")
        self.window_size_ms = window_size_ms
        self.value_field = value_field
        self.watermark = WatermarkTracker(allowed_lateness_ms)
        self._state: _AggregatorState = {}
        self._closed_windows: set[tuple[str, int]] = set()

    def _window_start(self, event_time_ms: int) -> int:
        return (event_time_ms // self.window_size_ms) * self.window_size_ms

    def add(self, event: Event) -> list[LateEvent]:
        """Add one event; returns a (possibly empty) list of LateEvents."""
        value = float(event.payload.get(self.value_field, 0))
        win_start = self._window_start(event.event_time_ms)
        win_end = win_start + self.window_size_ms

        # Late-arrival check: did this window already close?
        if win_end <= self.watermark.watermark_ms or (event.key, win_start) in self._closed_windows:
            return [
                LateEvent(
                    event=event,
                    window_start_ms=win_start,
                    window_end_ms=win_end,
                    watermark_ms=self.watermark.watermark_ms,
                )
            ]

        self.watermark.observe(event.event_time_ms, partition=event.partition)

        bucket_key = (event.key, win_start)
        bucket = self._state.get(bucket_key)
        if bucket is None:
            bucket = {"count": 0.0, "sum": 0.0, "min": float("inf"), "max": float("-inf")}
            self._state[bucket_key] = bucket
        bucket["count"] += 1
        bucket["sum"] += value
        if value < bucket["min"]:
            bucket["min"] = value
        if value > bucket["max"]:
            bucket["max"] = value
        return []

    def flush_ready(self) -> list[WindowOutput]:
        """Emit and clear windows whose right edge is below the watermark."""
        out: list[WindowOutput] = []
        wm = self.watermark.watermark_ms
        ready_keys = []
        for (key, win_start), bucket in self._state.items():
            win_end = win_start + self.window_size_ms
            if win_end <= wm:
                ready_keys.append((key, win_start))
                out.append(
                    WindowOutput(
                        window_start_ms=win_start,
                        window_end_ms=win_end,
                        key=key,
                        count=int(bucket["count"]),
                        sum_value=bucket["sum"],
                        min_value=bucket["min"] if bucket["count"] else 0.0,
                        max_value=bucket["max"] if bucket["count"] else 0.0,
                    )
                )
        for key in ready_keys:
            del self._state[key]
            self._closed_windows.add(key)
        return out

    def flush_all(self) -> list[WindowOutput]:
        """Emit every open window regardless of watermark (end-of-stream)."""
        out: list[WindowOutput] = []
        for (key, win_start), bucket in self._state.items():
            win_end = win_start + self.window_size_ms
            out.append(
                WindowOutput(
                    window_start_ms=win_start,
                    window_end_ms=win_end,
                    key=key,
                    count=int(bucket["count"]),
                    sum_value=bucket["sum"],
                    min_value=bucket["min"] if bucket["count"] else 0.0,
                    max_value=bucket["max"] if bucket["count"] else 0.0,
                )
            )
            self._closed_windows.add((key, win_start))
        self._state.clear()
        return out
