"""DuckDB sink unit tests."""

from __future__ import annotations

import json
from pathlib import Path

from streaming_etl import DuckDBSink, Event
from streaming_etl.watermark import LateEvent, WindowOutput


def test_write_and_count_windows(tmp_path):
    db = tmp_path / "t.duckdb"
    with DuckDBSink(db) as sink:
        n = sink.write_windows(
            [
                WindowOutput(0, 1000, "a", 3, 6.0, 1.0, 3.0),
                WindowOutput(1000, 2000, "a", 1, 9.0, 9.0, 9.0),
            ]
        )
        assert n == 2
        assert sink.count_windows() == 2


def test_upsert_on_same_key_window(tmp_path):
    db = tmp_path / "t.duckdb"
    with DuckDBSink(db) as sink:
        sink.write_windows([WindowOutput(0, 1000, "a", 1, 1.0, 1.0, 1.0)])
        # Re-write with updated counts (e.g., from a retry / replay)
        sink.write_windows([WindowOutput(0, 1000, "a", 5, 5.0, 1.0, 5.0)])
        rows = sink.fetch_windows()
        assert len(rows) == 1
        # column order: window_start, window_end, key, count, sum, min, max
        assert rows[0][3] == 5
        assert rows[0][4] == 5.0


def test_late_events_stored_with_payload_as_json(tmp_path):
    db = tmp_path / "t.duckdb"
    with DuckDBSink(db) as sink:
        evt = Event(key="a", event_time_ms=500, payload={"v": 7}, ingest_time_ms=999)
        sink.write_late(
            [LateEvent(event=evt, window_start_ms=0, window_end_ms=1000, watermark_ms=1500)]
        )
        rows = sink.fetch_late()
        assert len(rows) == 1
        # column order: key, event_time_ms, payload_json, window_start_ms, watermark_ms
        assert rows[0][0] == "a"
        assert json.loads(rows[0][2]) == {"v": 7}
        assert rows[0][4] == 1500
