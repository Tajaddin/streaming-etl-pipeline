"""Watermark + tumbling aggregator unit tests."""

from __future__ import annotations

from streaming_etl import Event, TumblingAggregator, WatermarkTracker


def test_watermark_is_monotonic():
    w = WatermarkTracker(allowed_lateness_ms=100)
    w.observe(1000)
    assert w.watermark_ms == 900
    # Late event arrives — watermark must NOT drop.
    w.observe(500)
    assert w.watermark_ms == 900


def test_watermark_advances_with_max_event_time():
    w = WatermarkTracker(allowed_lateness_ms=50)
    w.observe(1000)
    assert w.watermark_ms == 950
    w.observe(1200)
    assert w.watermark_ms == 1150


def test_idle_advance_pushes_watermark_from_wall_clock():
    w = WatermarkTracker(allowed_lateness_ms=100)
    w.observe(1000)  # watermark = 900
    w.advance_idle(5000)
    assert w.watermark_ms == 4900


def test_tumbling_aggregator_emits_window_when_watermark_passes():
    agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
    agg.add(Event(key="k", event_time_ms=500, payload={"v": 1}))
    agg.add(Event(key="k", event_time_ms=900, payload={"v": 2}))
    # Watermark is now 900 (no lateness); window [0,1000) not yet closed.
    assert agg.flush_ready() == []
    # Bump watermark past the window.
    agg.add(Event(key="k", event_time_ms=1500, payload={"v": 99}))
    closed = agg.flush_ready()
    assert len(closed) == 1
    w = closed[0]
    assert w.window_start_ms == 0
    assert w.window_end_ms == 1000
    assert w.count == 2
    assert w.sum_value == 3.0
    assert w.min_value == 1.0
    assert w.max_value == 2.0


def test_aggregator_counts_per_key():
    agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
    for i in range(5):
        agg.add(Event(key="A", event_time_ms=i * 100, payload={"v": 1}))
    for i in range(3):
        agg.add(Event(key="B", event_time_ms=i * 100, payload={"v": 2}))
    agg.add(Event(key="A", event_time_ms=2000, payload={"v": 99}))  # bumps watermark
    closed = sorted(agg.flush_ready(), key=lambda w: w.key)
    by_key = {w.key: w for w in closed}
    assert by_key["A"].count == 5
    assert by_key["A"].sum_value == 5.0
    assert by_key["B"].count == 3
    assert by_key["B"].sum_value == 6.0


def test_late_event_goes_to_side_output():
    agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
    agg.add(Event(key="k", event_time_ms=500, payload={"v": 1}))
    agg.add(Event(key="k", event_time_ms=2500, payload={"v": 2}))  # closes window [0,1000)
    _ = agg.flush_ready()
    # Now a stale event arrives, belongs to window [0,1000) which is closed.
    late = agg.add(Event(key="k", event_time_ms=750, payload={"v": 99}))
    assert len(late) == 1
    assert late[0].window_start_ms == 0
    assert late[0].window_end_ms == 1000
    assert late[0].event.payload == {"v": 99}


def test_allowed_lateness_keeps_window_open_briefly():
    agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=500)
    agg.add(Event(key="k", event_time_ms=500, payload={"v": 1}))
    # event_time 1200 → watermark 700, window [0,1000) NOT yet closed.
    agg.add(Event(key="k", event_time_ms=1200, payload={"v": 2}))
    # A "late" event within the grace period should still be admitted.
    late = agg.add(Event(key="k", event_time_ms=750, payload={"v": 7}))
    assert late == [], "event inside lateness window should not be flagged late"


def test_flush_all_emits_remaining_open_windows():
    agg = TumblingAggregator(window_size_ms=1000, value_field="v", allowed_lateness_ms=0)
    agg.add(Event(key="k", event_time_ms=200, payload={"v": 1}))
    agg.add(Event(key="k", event_time_ms=300, payload={"v": 2}))
    assert agg.flush_ready() == []  # watermark only at 300
    out = agg.flush_all()
    assert len(out) == 1
    assert out[0].count == 2


def test_zero_window_size_rejected():
    import pytest

    with pytest.raises(ValueError):
        TumblingAggregator(window_size_ms=0, value_field="v")
