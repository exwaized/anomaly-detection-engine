"""
engine.py — Core orchestrator for the Anomaly Detection Engine.

Manages:
- Metric stream ingestion (multi-source, concurrent)
- Parallel detector dispatch via ThreadPoolExecutor
- Shared rolling baseline state with RLock synchronization
- Alert aggregation and deduplication
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

from anomaly_engine.detectors.zscore import ZScoreDetector
from anomaly_engine.detectors.baseline_delta import BaselineDeltaDetector
from anomaly_engine.detectors.wtd import WTDDetector
from anomaly_engine.detectors.mtd import MTDDetector
from anomaly_engine.ingestion.stream import MetricStream
from anomaly_engine.alerting.dispatcher import AlertDispatcher
from anomaly_engine.utils.state import RollingStateStore
from anomaly_engine.utils.config import EngineConfig

logger = logging.getLogger(__name__)


class AnomalyDetectionEngine:
    """
    Multi-threaded anomaly detection engine.

    Architecture:
        - One ingestor thread per metric source (file queue / Kafka mock)
        - ThreadPoolExecutor dispatches N detector workers per metric batch
        - RollingStateStore holds shared baseline windows, protected by RLock
        - AlertDispatcher deduplicates and routes alerts

    Thread safety:
        - RollingStateStore uses per-dimension RLock
        - Alert queue is thread-safe (queue.Queue)
        - Detector instances are stateless; state is injected via StateStore
    """

    def __init__(self, config: EngineConfig):
        self.config = config
        self.state_store = RollingStateStore(
            window_days=config.baseline_window_days,
            max_dimensions=config.max_dimensions,
        )
        self.alert_dispatcher = AlertDispatcher(config=config)
        self.streams: list[MetricStream] = []
        self._shutdown_event = threading.Event()
        self._executor: ThreadPoolExecutor | None = None

        # Detectors are stateless — instantiate once, reuse across threads
        self.detectors = [
            ZScoreDetector(threshold=config.zscore_threshold),
            BaselineDeltaDetector(threshold_pct=config.baseline_delta_pct),
            WTDDetector(threshold_pct=config.wtd_threshold_pct),
            MTDDetector(threshold_pct=config.mtd_threshold_pct),
        ]

        logger.info(
            "Engine initialised | detectors=%d | workers=%d | window=%dd",
            len(self.detectors),
            config.max_workers,
            config.baseline_window_days,
        )

    def add_stream(self, stream: MetricStream) -> None:
        """Register a metric source with the engine."""
        self.streams.append(stream)
        logger.info("Stream registered: %s", stream.name)

    def run(self) -> None:
        """
        Start the engine. Blocks until shutdown() is called or KeyboardInterrupt.

        Flow:
            1. Spin up ThreadPoolExecutor
            2. Each stream runs its own ingestor loop in a daemon thread
            3. Each ingested metric batch is dispatched to all detectors in parallel
            4. Alerts are collected and dispatched asynchronously
        """
        if not self.streams:
            raise RuntimeError("No metric streams registered. Call add_stream() first.")

        self._executor = ThreadPoolExecutor(
            max_workers=self.config.max_workers,
            thread_name_prefix="detector-worker",
        )

        ingestor_threads = []
        for stream in self.streams:
            t = threading.Thread(
                target=self._ingestor_loop,
                args=(stream,),
                name=f"ingestor-{stream.name}",
                daemon=True,
            )
            t.start()
            ingestor_threads.append(t)
            logger.info("Ingestor started: %s", stream.name)

        logger.info("Engine running. Press Ctrl+C to stop.")
        try:
            while not self._shutdown_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            logger.info("Interrupt received — shutting down.")
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Graceful shutdown: drain queues, stop threads, flush alerts."""
        logger.info("Shutdown initiated.")
        self._shutdown_event.set()
        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=False)
        self.alert_dispatcher.flush()
        logger.info("Engine stopped cleanly.")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ingestor_loop(self, stream: MetricStream) -> None:
        """
        Continuously poll a MetricStream and dispatch batches for detection.
        Runs in a dedicated daemon thread per stream.
        """
        logger.debug("[%s] Ingestor loop started.", stream.name)
        while not self._shutdown_event.is_set():
            try:
                batch = stream.poll(timeout_sec=self.config.poll_interval_sec)
                if batch:
                    logger.debug(
                        "[%s] Polled %d metric points.", stream.name, len(batch)
                    )
                    self._dispatch_batch(batch, source=stream.name)
            except Exception:
                logger.exception("[%s] Error in ingestor loop.", stream.name)

    def _dispatch_batch(self, batch: list[dict[str, Any]], source: str) -> None:
        """
        Fan out each (metric_point, detector) pair to the thread pool.
        Collects futures and aggregates alerts without blocking the ingestor.
        """
        futures = {}
        for point in batch:
            # Update shared rolling state (thread-safe via RLock inside store)
            self.state_store.update(point)

            for detector in self.detectors:
                future = self._executor.submit(
                    self._run_detector, detector, point, source
                )
                futures[future] = (detector.__class__.__name__, point)

        # Collect results — non-blocking via as_completed with short timeout
        for future in as_completed(futures, timeout=30):
            detector_name, point = futures[future]
            try:
                alert = future.result()
                if alert:
                    self.alert_dispatcher.dispatch(alert)
            except Exception:
                logger.exception(
                    "Detector %s raised exception on point %s",
                    detector_name,
                    point,
                )

    def _run_detector(
        self, detector: Any, point: dict[str, Any], source: str
    ) -> dict[str, Any] | None:
        """
        Execute a single detector against one metric point.
        State is fetched from the shared store (read-only, no lock needed for reads).
        Returns an alert dict or None.
        """
        dimension_key = (point.get("country"), point.get("segment"), point.get("metric"))
        history = self.state_store.get_history(dimension_key)

        return detector.detect(point=point, history=history, source=source)
