"""
test_state.py — Concurrency tests for RollingStateStore.

Verifies:
  - Concurrent writes from multiple threads produce no data corruption
  - Per-dimension locking doesn't cause deadlocks
  - Window eviction works correctly at max capacity
"""

import threading
import pytest
from anomaly_engine.utils.state import RollingStateStore


class TestRollingStateStore:
    def test_basic_update_and_get(self):
        store = RollingStateStore(window_days=5)
        point = {"country": "Kenya", "segment": "b2b", "metric": "swaps", "value": 100.0}
        store.update(point)
        history = store.get_history(("Kenya", "b2b", "swaps"))
        assert history == [100.0]

    def test_window_eviction(self):
        store = RollingStateStore(window_days=3)
        point = {"country": "Kenya", "segment": "b2b", "metric": "swaps", "value": 0.0}
        for i in range(10):
            point = {**point, "value": float(i)}
            store.update(point)
        history = store.get_history(("Kenya", "b2b", "swaps"))
        assert len(history) == 3
        assert history == [7.0, 8.0, 9.0]

    def test_missing_dimension_returns_empty(self):
        store = RollingStateStore()
        result = store.get_history(("Uganda", "b2c", "dormant_bikes"))
        assert result == []

    def test_concurrent_writes_no_corruption(self):
        """
        50 threads each writing 100 points to the same dimension.
        History length should not exceed window_days and contain no None values.
        """
        store = RollingStateStore(window_days=30)
        key = ("Rwanda", "b2b", "active_bikes")
        errors = []

        def write_batch(start: int):
            try:
                for i in range(100):
                    store.update({
                        "country": "Rwanda",
                        "segment": "b2b",
                        "metric": "active_bikes",
                        "value": float(start + i),
                    })
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=write_batch, args=(i * 100,)) for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Exceptions in threads: {errors}"
        history = store.get_history(key)
        assert len(history) <= 30
        assert all(isinstance(v, float) for v in history)

    def test_independent_dimensions_dont_interfere(self):
        store = RollingStateStore(window_days=10)
        dims = [
            ("Kenya", "b2b", "swaps"),
            ("Uganda", "b2c", "active_bikes"),
            ("Rwanda", "b2b", "dormant_bikes"),
        ]

        def write(dim_key, value):
            country, segment, metric = dim_key
            for _ in range(5):
                store.update({"country": country, "segment": segment, "metric": metric, "value": value})

        threads = [threading.Thread(target=write, args=(dim, float(i + 1) * 100)) for i, dim in enumerate(dims)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for i, dim in enumerate(dims):
            history = store.get_history(dim)
            assert all(v == float(i + 1) * 100 for v in history), \
                f"Dimension {dim} contaminated: {history}"

    def test_max_dimensions_cap(self):
        store = RollingStateStore(window_days=10, max_dimensions=3)
        for i in range(5):
            store.update({"country": f"Country{i}", "segment": "b2b", "metric": "swaps", "value": 1.0})
        assert store.dimension_count() <= 3
