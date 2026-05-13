"""
dispatcher.py — Alert dispatcher with deduplication and multi-channel routing.

Responsibilities:
  - Receive alerts from detector workers (thread-safe via queue.Queue)
  - Deduplicate: suppress repeated alerts for same (dimension, detector)
    within a cooldown window
  - Route: log to console, write to JSONL file, (optionally) send email

Thread safety:
  - Alert queue is queue.Queue (thread-safe by design)
  - Dedup cache uses threading.Lock for safe concurrent access
"""

import json
import logging
import queue
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from anomaly_engine.utils.config import EngineConfig

logger = logging.getLogger(__name__)


class AlertDispatcher:
    """
    Thread-safe alert dispatcher with deduplication.

    Dedup logic:
        An alert is suppressed if the same (country, segment, metric, detector)
        combination fired within the last `cooldown_minutes` minutes.
        This prevents alert storms during sustained metric shifts.
    """

    def __init__(self, config: EngineConfig):
        self.config = config
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self._dedup_cache: dict[tuple, float] = {}
        self._dedup_lock = threading.Lock()
        self._alert_count = 0

        # Output file for persisted alerts
        if config.alert_output_path:
            Path(config.alert_output_path).parent.mkdir(parents=True, exist_ok=True)

        # Background thread drains the queue
        self._worker = threading.Thread(
            target=self._drain_loop,
            name="alert-dispatcher",
            daemon=True,
        )
        self._worker.start()

    def dispatch(self, alert: dict[str, Any]) -> None:
        """Enqueue an alert. Non-blocking; called from detector worker threads."""
        self._queue.put(alert)

    def flush(self) -> None:
        """Block until the alert queue is fully drained (called on shutdown)."""
        self._queue.join()
        logger.info("AlertDispatcher flushed. Total alerts dispatched: %d", self._alert_count)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _drain_loop(self) -> None:
        """Background loop: dedup → route → mark done."""
        while True:
            try:
                alert = self._queue.get(timeout=1.0)
                if self._should_dispatch(alert):
                    self._route(alert)
                    self._alert_count += 1
                self._queue.task_done()
            except queue.Empty:
                continue
            except Exception:
                logger.exception("Alert dispatcher error.")

    def _should_dispatch(self, alert: dict[str, Any]) -> bool:
        """Return True if alert passes deduplication check."""
        key = (
            alert.get("country"),
            alert.get("segment"),
            alert.get("metric"),
            alert.get("detector"),
        )
        cooldown_sec = self.config.alert_cooldown_minutes * 60
        now = time.time()

        with self._dedup_lock:
            last_fired = self._dedup_cache.get(key, 0)
            if now - last_fired < cooldown_sec:
                logger.debug("Alert suppressed (cooldown): %s", key)
                return False
            self._dedup_cache[key] = now
            return True

    def _route(self, alert: dict[str, Any]) -> None:
        """Route alert to all configured sinks."""
        self._log_alert(alert)
        if self.config.alert_output_path:
            self._write_jsonl(alert)

    def _log_alert(self, alert: dict[str, Any]) -> None:
        severity = alert.get("severity", "medium").upper()
        msg = (
            f"[ALERT:{severity}] detector={alert.get('detector')} | "
            f"country={alert.get('country')} | segment={alert.get('segment')} | "
            f"metric={alert.get('metric')} | value={alert.get('value')} | "
            f"ts={alert.get('timestamp')}"
        )
        if severity == "HIGH":
            logger.warning(msg)
        else:
            logger.info(msg)

        # Print extra context fields
        for key in ("z_score", "delta_pct", "baseline", "current_wtd", "prior_wtd", "current_mtd", "prior_mtd"):
            if key in alert:
                logger.info("  └─ %s = %s", key, alert[key])

    def _write_jsonl(self, alert: dict[str, Any]) -> None:
        try:
            with open(self.config.alert_output_path, "a") as f:
                f.write(json.dumps(alert) + "\n")
        except Exception:
            logger.exception("Failed to write alert to %s", self.config.alert_output_path)
