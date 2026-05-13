"""
zscore.py — Z-Score anomaly detector.

Flags a metric point if it deviates more than `threshold` standard deviations
from the mean of the rolling baseline window.

Thread safety: Stateless — all required history is passed in per call.
"""

import logging
import statistics
from typing import Any

logger = logging.getLogger(__name__)


class ZScoreDetector:
    """
    Z-Score detector.

    Algorithm:
        z = (value - mean(history)) / stdev(history)
        Alert if |z| > threshold

    Requires at least 3 historical data points to produce a meaningful result.
    """

    name = "zscore"

    def __init__(self, threshold: float = 3.0):
        self.threshold = threshold

    def detect(
        self,
        point: dict[str, Any],
        history: list[float],
        source: str,
    ) -> dict[str, Any] | None:
        value = point.get("value")
        if value is None or len(history) < 3:
            return None

        mean = statistics.mean(history)
        stdev = statistics.stdev(history)

        if stdev == 0:
            return None

        z = (value - mean) / stdev
        if abs(z) > self.threshold:
            logger.debug(
                "[%s] Z-score alert | dim=%s | z=%.2f | value=%.2f | mean=%.2f",
                source,
                (point.get("country"), point.get("segment"), point.get("metric")),
                z,
                value,
                mean,
            )
            return {
                "detector": self.name,
                "source": source,
                "country": point.get("country"),
                "segment": point.get("segment"),
                "metric": point.get("metric"),
                "value": value,
                "z_score": round(z, 4),
                "mean": round(mean, 4),
                "stdev": round(stdev, 4),
                "threshold": self.threshold,
                "timestamp": point.get("timestamp"),
                "severity": "high" if abs(z) > self.threshold * 1.5 else "medium",
            }
        return None
