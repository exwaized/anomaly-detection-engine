"""
mtd.py — Month-to-Date (MTD) comparison detector.

Compares current MTD cumulative value against prior month's MTD value
at the same day-of-month offset. Flags if percentage change exceeds threshold.

Thread safety: Stateless — all required history is passed in per call.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class MTDDetector:
    """
    Month-to-Date Comparison Detector.

    Algorithm:
        current_mtd = sum of values from day 1 of current month to today
        prior_mtd   = sum of values from day 1 of prior month to same day offset
        delta_pct   = (current_mtd - prior_mtd) / prior_mtd * 100
        Alert if |delta_pct| > threshold_pct

    history is expected to contain ordered daily values.
    Requires at least 60 data points (two months) for a meaningful comparison.
    """

    name = "mtd"

    def __init__(self, threshold_pct: float = 10.0):
        self.threshold_pct = threshold_pct

    def detect(
        self,
        point: dict[str, Any],
        history: list[float],
        source: str,
    ) -> dict[str, Any] | None:
        value = point.get("value")
        day_of_month = point.get("day_of_month")  # 1-indexed

        if value is None or day_of_month is None or len(history) < 32:
            return None

        # Current MTD: last (day_of_month - 1) history values + today's value
        days_elapsed = day_of_month
        current_slice = history[-(days_elapsed - 1):] + [value] if days_elapsed > 1 else [value]
        current_mtd = sum(current_slice)

        # Prior MTD: same day count, offset by ~30 days
        prior_offset = 30
        prior_start = -(days_elapsed + prior_offset - 1)
        prior_end = -prior_offset + 1 if prior_offset > 1 else None
        prior_slice = history[prior_start:prior_end]

        if not prior_slice or sum(prior_slice) == 0:
            return None

        prior_mtd = sum(prior_slice)
        delta_pct = (current_mtd - prior_mtd) / prior_mtd * 100

        if abs(delta_pct) > self.threshold_pct:
            logger.debug(
                "[%s] MTD alert | dim=%s | delta=%.1f%% | current_mtd=%.2f | prior_mtd=%.2f",
                source,
                (point.get("country"), point.get("segment"), point.get("metric")),
                delta_pct,
                current_mtd,
                prior_mtd,
            )
            return {
                "detector": self.name,
                "source": source,
                "country": point.get("country"),
                "segment": point.get("segment"),
                "metric": point.get("metric"),
                "value": value,
                "current_mtd": round(current_mtd, 4),
                "prior_mtd": round(prior_mtd, 4),
                "delta_pct": round(delta_pct, 2),
                "threshold_pct": self.threshold_pct,
                "day_of_month": day_of_month,
                "timestamp": point.get("timestamp"),
                "severity": "high" if abs(delta_pct) > self.threshold_pct * 1.5 else "medium",
            }
        return None
