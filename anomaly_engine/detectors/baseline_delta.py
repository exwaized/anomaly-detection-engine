"""
baseline_delta.py — 7-day rolling baseline delta detector.

Flags a metric point if it deviates from the 7-day rolling mean
by more than `threshold_pct` percent.

Thread safety: Stateless — all required history is passed in per call.
"""

import logging
import statistics
from typing import Any

logger = logging.getLogger(__name__)


class BaselineDeltaDetector:
    """
    7-Day Baseline Delta Detector.

    Algorithm:
        baseline = mean(last 7 days of history)
        delta_pct = (value - baseline) / baseline * 100
        Alert if |delta_pct| > threshold_pct
    """

    name = "baseline_delta"

    def __init__(self, threshold_pct: float = 20.0):
        self.threshold_pct = threshold_pct

    def detect(
        self,
        point: dict[str, Any],
        history: list[float],
        source: str,
    ) -> dict[str, Any] | None:
        value = point.get("value")
        if value is None or len(history) < 3:
            return None

        window = history[-7:] if len(history) >= 7 else history
        baseline = statistics.mean(window)

        if baseline == 0:
            return None

        delta_pct = (value - baseline) / baseline * 100

        if abs(delta_pct) > self.threshold_pct:
            logger.debug(
                "[%s] Baseline delta alert | dim=%s | delta=%.1f%% | value=%.2f | baseline=%.2f",
                source,
                (point.get("country"), point.get("segment"), point.get("metric")),
                delta_pct,
                value,
                baseline,
            )
            return {
                "detector": self.name,
                "source": source,
                "country": point.get("country"),
                "segment": point.get("segment"),
                "metric": point.get("metric"),
                "value": value,
                "baseline": round(baseline, 4),
                "delta_pct": round(delta_pct, 2),
                "threshold_pct": self.threshold_pct,
                "window_days": len(window),
                "timestamp": point.get("timestamp"),
                "severity": "high" if abs(delta_pct) > self.threshold_pct * 1.5 else "medium",
            }
        return None
