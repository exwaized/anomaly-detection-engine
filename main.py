"""
main.py — Entry point for the Anomaly Detection Engine.

Usage:
    python main.py                          # Run with default config + KafkaMock stream
    python main.py --config configs/prod.yaml
    python main.py --stream file --watch-dir data/incoming
    python main.py --anomaly-prob 0.1       # Higher anomaly injection for demo
"""

import argparse
import logging
import sys
from pathlib import Path

from anomaly_engine.engine import AnomalyDetectionEngine
from anomaly_engine.ingestion.stream import KafkaMockStream, FileQueueStream
from anomaly_engine.utils.config import EngineConfig

# ------------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(threadName)-20s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("outputs/engine.log", mode="a"),
    ],
)

# Suppress noisy third-party loggers
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Distributed Anomaly Detection Engine",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to YAML config file. If not provided, defaults are used.",
    )
    parser.add_argument(
        "--stream",
        choices=["mock", "file"],
        default="mock",
        help="Stream type: 'mock' uses KafkaMockStream; 'file' uses FileQueueStream.",
    )
    parser.add_argument(
        "--watch-dir",
        type=str,
        default="data/incoming",
        help="Directory to watch for JSONL files (only used with --stream file).",
    )
    parser.add_argument(
        "--anomaly-prob",
        type=float,
        default=0.05,
        help="Probability of injecting anomalies per metric point (mock stream only).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Override max_workers from config.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Load config
    if args.config:
        config = EngineConfig.from_yaml(args.config)
        logger.info("Config loaded from: %s", args.config)
    else:
        config = EngineConfig()
        logger.info("Using default config.")

    if args.workers:
        config.max_workers = args.workers

    # Ensure output directory exists
    Path("outputs").mkdir(exist_ok=True)

    # Build engine
    engine = AnomalyDetectionEngine(config=config)

    # Attach stream(s)
    if args.stream == "mock":
        stream = KafkaMockStream(
            name="kafka-mock",
            anomaly_probability=args.anomaly_prob,
        )
        engine.add_stream(stream)
        logger.info("KafkaMockStream attached | anomaly_prob=%.2f", args.anomaly_prob)
    else:
        stream = FileQueueStream(
            name="file-queue",
            watch_dir=args.watch_dir,
        )
        engine.add_stream(stream)
        logger.info("FileQueueStream attached | watch_dir=%s", args.watch_dir)

    # Run (blocks until Ctrl+C)
    engine.run()


if __name__ == "__main__":
    main()
