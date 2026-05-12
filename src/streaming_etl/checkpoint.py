"""Crash-safe checkpointing.

The pipeline periodically persists (a) the per-partition committed offsets
and (b) the current watermark. On restart, the pipeline restores both:
the bus resumes consumption from committed offsets, and the watermark is
seeded so windows finalised before the crash aren't re-emitted.

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
"""


class Checkpointer:
    """File-backed offset + watermark store."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._conn = sqlite3.connect(self.path, isolation_level=None)
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def save_offsets(self, offsets: dict[int, int]) -> None:
        rows = [(int(p), int(off)) for p, off in offsets.items()]
        with self._conn:
            self._conn.executemany(
                "INSERT INTO offsets (partition, offset) VALUES (?, ?) "
                "ON CONFLICT(partition) DO UPDATE SET offset=excluded.offset",
                rows,
            )

    def load_offsets(self) -> dict[int, int]:
        rows = self._conn.execute("SELECT partition, offset FROM offsets").fetchall()
        return {p: off for p, off in rows}

    def save_watermark(self, watermark_ms: int) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO pipeline_state (name, value) VALUES ('watermark_ms', ?) "
                "ON CONFLICT(name) DO UPDATE SET value=excluded.value",
                (int(watermark_ms),),
            )

    def load_watermark(self) -> int | None:
        row = self._conn.execute(
            "SELECT value FROM pipeline_state WHERE name='watermark_ms'"
        ).fetchone()
        return int(row[0]) if row else None
