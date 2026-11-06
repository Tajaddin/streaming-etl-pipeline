"""Crash-safe checkpointing.

The pipeline periodically persists, in one SQLite transaction, (a) the last
processed offset per partition, (b) the watermarks, and (c) every window that
is still open. Offsets are committed to the bus only after that transaction,
so the bus never skips past an event whose effect exists only in memory.

On restart the pipeline restores the watermarks and open windows, and drops
any redelivered event at or below the checkpointed offset: its effect is
already in the restored state or in the sink.

We use SQLite (stdlib only) — DuckDB is the analytics sink; a separate
stateful KV is the right tool for "small, frequent, durable writes."
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


_SCHEMA = """
CREATE TABLE IF NOT EXISTS offsets (
    partition INTEGER PRIMARY KEY,
    offset    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_state (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS partition_watermarks (
    partition         INTEGER PRIMARY KEY,
    max_event_time_ms INTEGER NOT NULL,
    watermark_ms      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS open_windows (
    key             TEXT    NOT NULL,
    window_start_ms INTEGER NOT NULL,
    count           INTEGER NOT NULL,
    sum_value       REAL    NOT NULL,
    min_value       REAL    NOT NULL,
    max_value       REAL    NOT NULL,
    PRIMARY KEY (key, window_start_ms)
);
"""

_UPSERT_OFFSET = (
    "INSERT INTO offsets (partition, offset) VALUES (?, ?) "
    "ON CONFLICT(partition) DO UPDATE SET offset=excluded.offset"
)
_UPSERT_WATERMARK = (
    "INSERT INTO pipeline_state (name, value) VALUES ('watermark_ms', ?) "
    "ON CONFLICT(name) DO UPDATE SET value=excluded.value"
)

OpenWindow = tuple[str, int, int, float, float, float]


class Checkpointer:
    """File-backed store for offsets, watermarks and open-window state."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._conn = sqlite3.connect(self.path, isolation_level=None)
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def save_state(
        self,
        offsets: dict[int, int],
        watermark_ms: int,
        partition_watermarks: dict[int, tuple[int, int]],
        open_windows: list[OpenWindow],
    ) -> None:
        """Replace the whole checkpoint atomically."""
        conn = self._conn
        conn.execute("BEGIN")
        try:
            conn.executemany(
                _UPSERT_OFFSET, [(int(p), int(off)) for p, off in offsets.items()]
            )
            conn.execute(_UPSERT_WATERMARK, (int(watermark_ms),))
            conn.execute("DELETE FROM partition_watermarks")
            conn.executemany(
                "INSERT INTO partition_watermarks VALUES (?, ?, ?)",
                [(p, max_t, wm) for p, (max_t, wm) in partition_watermarks.items()],
            )
            conn.execute("DELETE FROM open_windows")
            conn.executemany(
                "INSERT INTO open_windows VALUES (?, ?, ?, ?, ?, ?)", open_windows
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def save_offsets(self, offsets: dict[int, int]) -> None:
        rows = [(int(p), int(off)) for p, off in offsets.items()]
        with self._conn:
            self._conn.executemany(_UPSERT_OFFSET, rows)

    def load_offsets(self) -> dict[int, int]:
        rows = self._conn.execute("SELECT partition, offset FROM offsets").fetchall()
        return {p: off for p, off in rows}

    def save_watermark(self, watermark_ms: int) -> None:
        with self._conn:
            self._conn.execute(_UPSERT_WATERMARK, (int(watermark_ms),))

    def load_watermark(self) -> int | None:
        row = self._conn.execute(
            "SELECT value FROM pipeline_state WHERE name='watermark_ms'"
        ).fetchone()
        return int(row[0]) if row else None

    def load_partition_watermarks(self) -> dict[int, tuple[int, int]]:
        rows = self._conn.execute(
            "SELECT partition, max_event_time_ms, watermark_ms FROM partition_watermarks"
        ).fetchall()
        return {p: (max_t, wm) for p, max_t, wm in rows}

    def load_open_windows(self) -> list[OpenWindow]:
        return self._conn.execute(
            "SELECT key, window_start_ms, count, sum_value, min_value, max_value "
            "FROM open_windows"
        ).fetchall()
