"""
config.py — Engine configuration dataclass.

Load from YAML via EngineConfig.from_yaml() or construct programmatically.
"""

from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class EngineConfig:
    # Concurrency
    max_workers: int = 8

    # Rolling window
    baseline_window_days: int = 30
    max_dimensions: int = 500

    # Detector thresholds
    zscore_threshold: float = 3.0
    baseline_delta_pct: float = 20.0
    wtd_threshold_pct: float = 15.0
    mtd_threshold_pct: float = 10.0

    # Alerting
    alert_cooldown_minutes: float = 60.0
    alert_output_path: Optional[str] = "outputs/alerts.jsonl"

    # Ingestion
    poll_interval_sec: float = 2.0

    @classmethod
    def from_yaml(cls, path: str) -> EngineConfig:
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def to_yaml(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(
                {k: getattr(self, k) for k in self.__dataclass_fields__},
                f,
                default_flow_style=False,
            )
