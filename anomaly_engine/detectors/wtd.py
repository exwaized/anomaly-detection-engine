"""
wtd.py — Week-to-Date (WTD) comparison detector.

Compares current WTD cumulative value against prior week's WTD value
at the same weekday offset. Flags if the percentage change exceeds threshold.

Thread safety: Stateless — all required history is passed in per call.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class WTDDetector:
    """
    Week-to-Date Comparison Detector.

    Algorithm:
        current_wtd  = sum of values from Monday of current week to today
        prior_wtd    = sum of values from Monday of prior week to same weekday offset
        delta_pct    = (current_wtd - prior_wtd) / prior_wtd * 100
        Alert if |delta_pct| > threshold_pct

    history is expected to contain ordered daily values.
    Requires at least 14 data points for a meaningful prior-week comparison.
    """

    name = "wtd"

    def __init__(self, threshold_pct: float = 15.0):
        self.threshold_pct = threshold_pct

    def detect(
        self,
        point: dict[str, Any],
        history: list[float],
        source: str,
    ) -> dict[str, Any] | None:
        value = point.get("value")
        weekday = point.get("weekday")  # 0=Mon, 6=Sun

        if value is None or weekday is None or len(history) < 14:
            return None

        # Days elapsed in current week = weekday index + 1 (Mon=1, Tue=2, ...)
        days_into_week = weekday + 1

        # Current WTD: last `days_into_week` values including today
        current_wtd_values = history[-(days_into_week - 1):] + [value] if days_into_week > 1 else [value]
        current_wtd = sum(current_wtd_values)

        # Prior WTD: same number of days, one week earlier
        prior_start = -(days_into_week + 7)
        prior_end = -7 if days_into_week > 0 else None
        prior_wtd_values = history[prior_start:prior_end]

        if not prior_wtd_values or sum(prior_wtd_values) == 0:
            return None

        prior_wtd = sum(prior_wtd_values)
        delta_pct = (current_wtd - prior_wtd) / prior_wtd * 100

        if abs(delta_pct) > self.threshold_pct:
            logger.debug(
                "[%s] WTD alert | dim=%s | delta=%.1f%% | current_wtd=%.2f | prior_wtd=%.2f",
                source,
                (point.get("country"), point.get("segment"), point.get("metric")),
                delta_pct,
                current_wtd,
                prior_wtd,
            )
            return {
                "detector": self.name,
                "source": source,
                "country": point.get("country"),
                "segment": point.get("segment"),
                "metric": point.get("metric"),
                "value": value,
                "current_wtd": round(current_wtd, 4),
                "prior_wtd": round(prior_wtd, 4),
                "delta_pct": round(delta_pct, 2),
                "threshold_pct": self.threshold_pct,
                "weekday": weekday,
                "timestamp": point.get("timestamp"),
                "severity": "high" if abs(delta_pct) > self.threshold_pct * 1.5 else "medium",
            }
        return None
