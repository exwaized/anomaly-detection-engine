"""
test_detectors.py — Unit tests for all four anomaly detectors.

Tests cover:
  - Normal values (no alert expected)
  - Anomalous values (alert expected)
  - Edge cases: insufficient history, zero stdev, zero baseline
"""

import pytest
from datetime import datetime

from anomaly_engine.detectors.zscore import ZScoreDetector
from anomaly_engine.detectors.baseline_delta import BaselineDeltaDetector
from anomaly_engine.detectors.wtd import WTDDetector
from anomaly_engine.detectors.mtd import MTDDetector


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

def make_point(value: float, weekday: int = 2, day_of_month: int = 10) -> dict:
    return {
        "country": "Kenya",
        "segment": "b2b",
        "metric": "billable_swaps",
        "value": value,
        "timestamp": datetime.utcnow().isoformat(),
        "weekday": weekday,
        "day_of_month": day_of_month,
    }


def normal_history(mean: float = 1000.0, length: int = 30) -> list[float]:
    """Stable history with no variance."""
    return [mean] * length


# ------------------------------------------------------------------
# ZScoreDetector
# ------------------------------------------------------------------

class TestZScoreDetector:
    def setup_method(self):
        self.det = ZScoreDetector(threshold=3.0)

    def test_normal_value_no_alert(self):
        history = [1000 + (i % 5) for i in range(30)]
        point = make_point(value=1002.0)
        result = self.det.detect(point, history, source="test")
        assert result is None

    def test_extreme_high_triggers_alert(self):
        history = normal_history(1000) + [1 for _ in range(5)]  # stdev > 0
        import statistics
        history = [1000 + (i % 20) - 10 for i in range(30)]
        point = make_point(value=5000.0)
        result = self.det.detect(point, history, source="test")
        assert result is not None
        assert result["detector"] == "zscore"
        assert result["z_score"] > 3.0

    def test_extreme_low_triggers_alert(self):
        history = [1000 + (i % 20) - 10 for i in range(30)]
        point = make_point(value=1.0)
        result = self.det.detect(point, history, source="test")
        assert result is not None
        assert result["z_score"] < -3.0

    def test_insufficient_history_returns_none(self):
        point = make_point(value=9999.0)
        result = self.det.detect(point, [1000, 1001], source="test")
        assert result is None

    def test_zero_stdev_returns_none(self):
        history = [1000.0] * 30
        point = make_point(value=9999.0)
        result = self.det.detect(point, history, source="test")
        assert result is None

    def test_alert_has_required_fields(self):
        history = [1000 + (i % 20) - 10 for i in range(30)]
        point = make_point(value=5000.0)
        result = self.det.detect(point, history, source="test")
        assert result is not None
        for field in ("detector", "country", "segment", "metric", "value", "z_score", "severity"):
            assert field in result

    def test_severity_high_for_extreme_zscore(self):
        history = [1000 + (i % 20) - 10 for i in range(30)]
        point = make_point(value=10000.0)
        result = self.det.detect(point, history, source="test")
        assert result is not None
        assert result["severity"] == "high"


# ------------------------------------------------------------------
# BaselineDeltaDetector
# ------------------------------------------------------------------

class TestBaselineDeltaDetector:
    def setup_method(self):
        self.det = BaselineDeltaDetector(threshold_pct=20.0)

    def test_normal_value_no_alert(self):
        history = normal_history(1000)
        point = make_point(value=1050.0)  # 5% above baseline
        assert self.det.detect(point, history, source="test") is None

    def test_high_value_triggers_alert(self):
        history = normal_history(1000)
        point = make_point(value=1500.0)  # 50% above baseline
        result = self.det.detect(point, history, source="test")
        assert result is not None
        assert result["delta_pct"] > 20.0

    def test_low_value_triggers_alert(self):
        history = normal_history(1000)
        point = make_point(value=700.0)  # 30% below baseline
        result = self.det.detect(point, history, source="test")
        assert result is not None
        assert result["delta_pct"] < -20.0

    def test_zero_baseline_returns_none(self):
        history = [0.0] * 30
        point = make_point(value=100.0)
        assert self.det.detect(point, history, source="test") is None

    def test_uses_last_7_days(self):
        # History of 1000 for 23 days, then spike to 5000 for 7 days
        history = [1000.0] * 23 + [5000.0] * 7
        point = make_point(value=5100.0)  # Only ~2% above the recent 7-day baseline
        result = self.det.detect(point, history, source="test")
        assert result is None  # Should not fire since value matches recent baseline


# ------------------------------------------------------------------
# WTDDetector
# ------------------------------------------------------------------

class TestWTDDetector:
    def setup_method(self):
        self.det = WTDDetector(threshold_pct=15.0)

    def test_insufficient_history_returns_none(self):
        point = make_point(value=1000.0, weekday=2)
        assert self.det.detect(point, [1000.0] * 10, source="test") is None

    def test_normal_wtd_no_alert(self):
        history = normal_history(1000, length=30)
        point = make_point(value=1000.0, weekday=2)
        result = self.det.detect(point, history, source="test")
        assert result is None

    def test_anomalous_wtd_triggers_alert(self):
        history = normal_history(1000, length=30)
        # Value 10x higher than baseline — WTD will spike
        point = make_point(value=10000.0, weekday=0)  # Monday, single-day WTD
        result = self.det.detect(point, history, source="test")
        assert result is not None
        assert abs(result["delta_pct"]) > 15.0


# ------------------------------------------------------------------
# MTDDetector
# ------------------------------------------------------------------

class TestMTDDetector:
    def setup_method(self):
        self.det = MTDDetector(threshold_pct=10.0)

    def test_insufficient_history_returns_none(self):
        point = make_point(value=1000.0, day_of_month=5)
        assert self.det.detect(point, [1000.0] * 20, source="test") is None

    def test_normal_mtd_no_alert(self):
        history = normal_history(1000, length=60)
        point = make_point(value=1000.0, day_of_month=5)
        result = self.det.detect(point, history, source="test")
        assert result is None

    def test_anomalous_mtd_triggers_alert(self):
        # Prior month had low values; current month is 10x higher
        history = [100.0] * 60
        point = make_point(value=5000.0, day_of_month=1)
        result = self.det.detect(point, history, source="test")
        assert result is not None
        assert abs(result["delta_pct"]) > 10.0
