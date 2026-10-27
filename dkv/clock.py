"""Hybrid logical clock (Kulkarni et al., 2014), packed into one 64-bit integer.

Versions must be totally ordered for last-writer-wins, and should follow
wall-clock time so "latest" means what users expect. Plain wall clocks can
go backwards (NTP adjustments) or disagree between nodes; an HLC never goes
backwards, and after a node *sees* a version it always issues larger ones,
so a write that causally follows another always wins against it.

Layout: high 48 bits = milliseconds since the epoch, low 16 bits = counter.
"""
from __future__ import annotations

import threading
import time
from typing import Callable

COUNTER_BITS = 16
COUNTER_MASK = (1 << COUNTER_BITS) - 1


class HybridClock:
    def __init__(self, wall_ms: Callable[[], int] | None = None):
        self._wall = wall_ms or (lambda: time.time_ns() // 1_000_000)
        self._last = 0
        self._lock = threading.Lock()

    def now(self) -> int:
        """A timestamp greater than every timestamp this clock has issued or observed."""
        with self._lock:
            wall = self._wall() << COUNTER_BITS
            self._last = wall if wall > self._last else self._last + 1
            return self._last

    def observe(self, ts: int) -> None:
        """Advance past a timestamp seen from another node."""
        with self._lock:
            if ts > self._last:
                self._last = ts


def wall_ms(ts: int) -> int:
    return ts >> COUNTER_BITS
