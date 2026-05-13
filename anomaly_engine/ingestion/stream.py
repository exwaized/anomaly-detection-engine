"""
stream.py — Metric stream abstraction.

Provides:
  - MetricStream: abstract base class
  - FileQueueStream: reads JSONL files from a directory (simulates a file queue)
  - KafkaMockStream: generates synthetic metrics for local testing

Both implement a poll(timeout_sec) interface so the engine ingestor loop
is source-agnostic.
"""

import json
import logging
import math
import os
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class MetricStream(ABC):
    """Abstract metric stream. Implement poll() to ingest from any source."""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def poll(self, timeout_sec: float = 1.0) -> list[dict[str, Any]]:
        """
        Return a batch of metric points. Block up to timeout_sec.
        Each point must contain at minimum:
            - country (str)
            - segment (str)
            - metric (str)
            - value (float)
            - timestamp (ISO str)
            - weekday (int, 0=Mon)
            - day_of_month (int, 1-indexed)
        """
        ...


class FileQueueStream(MetricStream):
    """
    Polls a directory for JSONL files. Each line is one metric point.
    Files are processed in mtime order and deleted after ingestion.

    Use case: drop metric export files into a watched folder from any ETL job.
    """

    def __init__(self, name: str, watch_dir: str, poll_interval: float = 2.0):
        super().__init__(name)
        self.watch_dir = Path(watch_dir)
        self.poll_interval = poll_interval
        self._last_poll = 0.0
        self.watch_dir.mkdir(parents=True, exist_ok=True)

    def poll(self, timeout_sec: float = 1.0) -> list[dict[str, Any]]:
        now = time.time()
        if now - self._last_poll < self.poll_interval:
            time.sleep(min(timeout_sec, self.poll_interval))
            return []

        self._last_poll = now
        points = []

        files = sorted(self.watch_dir.glob("*.jsonl"), key=lambda f: f.stat().st_mtime)
        for fpath in files:
            try:
                with open(fpath) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            points.append(json.loads(line))
                os.remove(fpath)
                logger.debug("[%s] Ingested %s", self.name, fpath.name)
            except Exception:
                logger.exception("[%s] Failed to read %s", self.name, fpath)

        return points


class KafkaMockStream(MetricStream):
    """
    Synthetic metric generator for local development and testing.

    Simulates realistic EV fleet metrics (billable_swaps, active_bikes,
    dormant_bikes) across multiple countries and segments with injected
    anomalies at configurable frequency.

    Mimics the cadence of a Kafka consumer poll loop.
    """

    COUNTRIES = ["Kenya", "Uganda", "Rwanda"]
    SEGMENTS = ["b2b", "b2c"]
    METRICS = {
        "billable_swaps": (800, 80),    # (mean, stdev)
        "active_bikes": (3000, 200),
        "dormant_bikes": (400, 60),
        "reactivation_rate": (0.12, 0.02),
    }

    def __init__(
        self,
        name: str,
        anomaly_probability: float = 0.05,
        batch_size: int = 12,
    ):
        super().__init__(name)
        self.anomaly_probability = anomaly_probability
        self.batch_size = batch_size
        self._histories: dict[tuple, list[float]] = {}
        self._day_offset = 0

    def poll(self, timeout_sec: float = 1.0) -> list[dict[str, Any]]:
        time.sleep(timeout_sec)
        points = []
        now = datetime.utcnow() + timedelta(days=self._day_offset)
        self._day_offset += 1

        for country in self.COUNTRIES:
            for segment in self.SEGMENTS:
                for metric, (mean, stdev) in self.METRICS.items():
                    key = (country, segment, metric)

                    # Build up plausible history first
                    if key not in self._histories:
                        self._histories[key] = [
                            max(0, random.gauss(mean, stdev)) for _ in range(30)
                        ]

                    # Inject anomaly with configured probability
                    if random.random() < self.anomaly_probability:
                        multiplier = random.choice([0.4, 0.5, 1.8, 2.2, 3.0])
                        value = mean * multiplier
                        logger.info(
                            "[%s] Injecting anomaly | %s/%s/%s | value=%.1f (mean=%.1f)",
                            self.name, country, segment, metric, value, mean,
                        )
                    else:
                        value = max(0, random.gauss(mean, stdev * 0.5))

                    self._histories[key].append(value)

                    points.append({
                        "country": country,
                        "segment": segment,
                        "metric": metric,
                        "value": round(value, 4),
                        "timestamp": now.isoformat(),
                        "weekday": now.weekday(),
                        "day_of_month": now.day,
                        "source": self.name,
                    })

        return points
