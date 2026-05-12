"""streaming-etl-pipeline — event-time streaming ETL into DuckDB.

Exports:

* :class:`Event` — incoming event record (key, event_time, payload).
* :class:`InMemoryBus`, :class:`KafkaBus` — Kafka-shape source / sink behind a single
  :class:`MessageBus` protocol.
* :class:`WatermarkTracker` — bounded-out-of-orderness watermark policy.
* :class:`TumblingAggregator` — event-time tumbling windows with late-data side output.
* :class:`DuckDBSink` — writes finalized windows + late events to separate tables.
* :class:`Checkpointer` — SQLite-backed offset + watermark persistence for crash-safe resume.
* :class:`Pipeline` — composes source → aggregator → sink + checkpointer.
"""

from streaming_etl.bus import InMemoryBus, KafkaBus, MessageBus
from streaming_etl.checkpoint import Checkpointer
from streaming_etl.event import Event
from streaming_etl.pipeline import Pipeline, PipelineStats
from streaming_etl.sink import DuckDBSink
from streaming_etl.watermark import (
    LateEvent,
    TumblingAggregator,
    WatermarkTracker,
    WindowOutput,
)

__all__ = [
    "Checkpointer",
    "DuckDBSink",
    "Event",
    "InMemoryBus",
    "KafkaBus",
    "LateEvent",
    "MessageBus",
    "Pipeline",
    "PipelineStats",
    "TumblingAggregator",
    "WatermarkTracker",
    "WindowOutput",
]
