"""DuckDB sink for finalized windows + late events.

Two tables:

* ``windows`` — one row per closed window, with (key, window_start, window_end,
  count, sum, min, max). Partition column for downstream partition pruning.
* ``late_events`` — one row per late event, with the original payload as
  JSON, the window it would have landed in, and the watermark at arrival.

The sink uses prepared parameterised INSERTs, batched per ``flush()`` call.
Schema is idempotent — running the sink twice against the same file just
extends both tables.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import duckdb

from streaming_etl.watermark import LateEvent, WindowOutput


_SCHEMA_WINDOWS = """
CREATE TABLE IF NOT EXISTS windows (
    window_start_ms BIGINT NOT NULL,
    window_end_ms   BIGINT NOT NULL,
    key             VARCHAR NOT NULL,
    count           BIGINT NOT NULL,
    sum_value       DOUBLE NOT NULL,
    min_value       DOUBLE NOT NULL,
    max_value       DOUBLE NOT NULL,
    PRIMARY KEY (key, window_start_ms)
)
"""

_SCHEMA_LATE = """
CREATE TABLE IF NOT EXISTS late_events (
    key             VARCHAR NOT NULL,
    event_time_ms   BIGINT NOT NULL,
    ingest_time_ms  BIGINT NOT NULL,
    payload_json    VARCHAR NOT NULL,
    window_start_ms BIGINT NOT NULL,
    window_end_ms   BIGINT NOT NULL,
    watermark_ms    BIGINT NOT NULL
)
"""


class DuckDBSink:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._conn = duckdb.connect(self.db_path)
        self._conn.execute(_SCHEMA_WINDOWS)
        self._conn.execute(_SCHEMA_LATE)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DuckDBSink":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def write_windows(self, windows: Iterable[WindowOutput]) -> int:
        rows = [
            (
                w.window_start_ms,
                w.window_end_ms,
                w.key,
                w.count,
                w.sum_value,
                w.min_value,
                w.max_value,
            )
            for w in windows
        ]
        if not rows:
            return 0
        self._conn.execute("BEGIN TRANSACTION")
        try:
            self._conn.executemany(
                """
                INSERT INTO windows (window_start_ms, window_end_ms, key, count, sum_value, min_value, max_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (key, window_start_ms) DO UPDATE SET
                    count = excluded.count,
                    sum_value = excluded.sum_value,
                    min_value = excluded.min_value,
                    max_value = excluded.max_value,
                    window_end_ms = excluded.window_end_ms
                """,
                rows,
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return len(rows)

    def write_late(self, late: Iterable[LateEvent]) -> int:
        rows = [
            (
                l.event.key,
                l.event.event_time_ms,
                l.event.ingest_time_ms,
                json.dumps(l.event.payload, ensure_ascii=False),
                l.window_start_ms,
                l.window_end_ms,
                l.watermark_ms,
            )
            for l in late
        ]
        if not rows:
            return 0
        self._conn.execute("BEGIN TRANSACTION")
        try:
            self._conn.executemany(
                """
                INSERT INTO late_events (key, event_time_ms, ingest_time_ms, payload_json,
                                         window_start_ms, window_end_ms, watermark_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return len(rows)

    def count_windows(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM windows").fetchone()[0]

    def count_late(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM late_events").fetchone()[0]

    def fetch_windows(self) -> list[tuple]:
        return self._conn.execute(
            "SELECT window_start_ms, window_end_ms, key, count, sum_value, min_value, max_value "
            "FROM windows ORDER BY window_start_ms, key"
        ).fetchall()

    def fetch_late(self) -> list[tuple]:
        return self._conn.execute(
            "SELECT key, event_time_ms, payload_json, window_start_ms, watermark_ms "
            "FROM late_events ORDER BY event_time_ms"
        ).fetchall()
