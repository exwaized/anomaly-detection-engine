"""
state.py — Thread-safe rolling state store.

Maintains a fixed-length sliding window of historical metric values
per (country, segment, metric) dimension tuple.

Thread safety:
  - One RLock per dimension key prevents cross-dimension contention
  - Global lock only used when creating a new dimension slot
  - Read operations acquire RLock in shared mode (RLock allows re-entrant reads)
"""

import logging
import threading
from collections import deque
from typing import Any

logger = logging.getLogger(__name__)


class RollingStateStore:
    """
    Per-dimension rolling window store.

    Design:
        - Each dimension (country, segment, metric) gets its own deque
          of fixed length `window_days`
        - Each deque is protected by its own RLock, so reads/writes to
          different dimensions never block each other
        - A global creation lock prevents race conditions when two threads
          encounter a new dimension simultaneously
    """

    def __init__(self, window_days: int = 30, max_dimensions: int = 500):
        self.window_days = window_days
        self.max_dimensions = max_dimensions

        self._windows: dict[tuple, deque] = {}
        self._locks: dict[tuple, threading.RLock] = {}
        self._creation_lock = threading.Lock()

    def update(self, point: dict[str, Any]) -> None:
        """Append a new value to the dimension's rolling window."""
        key = self._key(point)
        value = point.get("value")
        if value is None:
            return

        lock = self._get_or_create_lock(key)
        with lock:
            # If key was dropped due to max_dimensions cap, _windows won't have it
            if key in self._windows:
                self._windows[key].append(float(value))

    def get_history(self, key: tuple) -> list[float]:
        """Return a snapshot of the rolling window for a dimension key."""
        lock = self._locks.get(key)
        if lock is None:
            return []
        with lock:
            return list(self._windows.get(key, []))

    def dimension_count(self) -> int:
        """Number of active dimension slots."""
        with self._creation_lock:
            return len(self._windows)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _key(point: dict[str, Any]) -> tuple:
        return (
            point.get("country"),
            point.get("segment"),
            point.get("metric"),
        )

    def _get_or_create_lock(self, key: tuple) -> threading.RLock:
        """
        Return existing lock for key, or create one atomically.
        Double-checked locking pattern to avoid global lock contention
        on the hot path (update is called per metric point).
        """
        if key in self._locks:
            return self._locks[key]

        with self._creation_lock:
            # Re-check inside the lock (classic double-checked locking)
            if key not in self._locks:
                if len(self._windows) >= self.max_dimensions:
                    logger.warning(
                        "Max dimensions (%d) reached. Dropping key: %s",
                        self.max_dimensions, key,
                    )
                    return threading.RLock()  # Ephemeral lock, not stored
                self._windows[key] = deque(maxlen=self.window_days)
                self._locks[key] = threading.RLock()
                logger.debug("New dimension registered: %s", str(key))

        return self._locks[key]
