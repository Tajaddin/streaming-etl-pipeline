"""Event record shape.

Every record on the bus has:

* ``key`` — sharding key (e.g. user_id, device_id)
* ``event_time_ms`` — when the event happened, in epoch milliseconds. THIS is
  what windows align to, not when we received it.
* ``ingest_time_ms`` — when we received it (filled by the source).
* ``payload`` — opaque dict; pipelines extract specific fields downstream.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Event:
    key: str
    event_time_ms: int
    payload: dict[str, Any] = field(default_factory=dict)
    ingest_time_ms: int = 0
    partition: int = 0
    offset: int = 0
