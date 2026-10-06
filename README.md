# Anomaly Detection Engine

A **distributed, multi-threaded anomaly detection microservice** for business metric streams. Built to demonstrate production-grade concurrency design, algorithmic detection across multiple statistical methods, and reliable alert dispatch.

---

## Architecture

```
                        ┌──────────────────────────────┐
                        │       AnomalyDetectionEngine  │
                        │                              │
  ┌──────────────┐      │  ┌──────────────────────┐   │
  │ MetricStream │──────┼─▶│   Ingestor Thread     │   │
  │ (per source) │      │  │  (one per stream)     │   │
  └──────────────┘      │  └──────────┬───────────┘   │
                        │             │ batch          │
                        │             ▼                │
                        │  ┌──────────────────────┐   │
                        │  │  ThreadPoolExecutor   │   │
                        │  │  (N detector workers) │   │
                        │  └──────────┬───────────┘   │
                        │             │                │
                        │    ┌────────┼────────┐       │
                        │    ▼        ▼        ▼       │
                        │  ZScore  Baseline  WTD  MTD  │
                        │    └────────┬────────┘       │
                        │             │ alerts         │
                        │             ▼                │
                        │  ┌──────────────────────┐   │
                        │  │   AlertDispatcher     │   │
                        │  │  (dedup + route)      │   │
                        │  └──────────────────────┘   │
                        └──────────────────────────────┘
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| **ThreadPoolExecutor** for detectors | Detectors are I/O-free CPU tasks; a fixed pool avoids unbounded thread creation |
| **Per-dimension RLock** in StateStore | Eliminates cross-dimension contention; dimensions are independent |
| **Double-checked locking** for new dimensions | Minimises global lock contention on the hot update path |
| **queue.Queue** for alerts | Thread-safe FIFO; decouples detector threads from dispatcher I/O |
| **Stateless detectors** | Enables safe reuse across threads without synchronization |
| **Alert cooldown cache** | Prevents alert storms during sustained metric anomalies |

---

## Detection Algorithms

### 1. Z-Score Detector
```
z = (value − mean(history)) / stdev(history)
Alert if |z| > threshold (default: 3.0)
```
Best for: sudden point spikes or drops relative to recent distribution.

### 2. Baseline Delta Detector
```
baseline = mean(last 7 days of history)
delta_pct = (value − baseline) / baseline × 100
Alert if |delta_pct| > threshold (default: 20%)
```
Best for: sustained level shifts from recent rolling mean.

### 3. WTD (Week-to-Date) Detector
```
current_wtd = Σ(values from Monday to today)
prior_wtd   = Σ(same weekday span, one week ago)
delta_pct   = (current_wtd − prior_wtd) / prior_wtd × 100
Alert if |delta_pct| > threshold (default: 15%)
```
Best for: week-over-week cumulative performance tracking.

### 4. MTD (Month-to-Date) Detector
```
current_mtd = Σ(values from day 1 of current month to today)
prior_mtd   = Σ(same day-of-month span, prior month)
delta_pct   = (current_mtd − prior_mtd) / prior_mtd × 100
Alert if |delta_pct| > threshold (default: 10%)
```
Best for: month-over-month cumulative anomaly detection.

---

## Project Structure

```
anomaly-detection-engine/
├── anomaly_engine/
│   ├── engine.py               # Core orchestrator (ThreadPoolExecutor, ingestor threads)
│   ├── detectors/
│   │   ├── zscore.py           # Z-Score detector
│   │   ├── baseline_delta.py   # 7-day rolling baseline detector
│   │   ├── wtd.py              # Week-to-date detector
│   │   └── mtd.py              # Month-to-date detector
│   ├── ingestion/
│   │   └── stream.py           # MetricStream, FileQueueStream, KafkaMockStream
│   ├── alerting/
│   │   └── dispatcher.py       # Thread-safe alert dispatcher with dedup
│   └── utils/
│       ├── state.py            # RollingStateStore (per-dimension RLock)
│       └── config.py           # EngineConfig dataclass
├── tests/
│   ├── test_detectors.py       # Unit tests for all detectors
│   └── test_state.py           # Concurrency tests for StateStore
├── configs/
│   └── default.yaml            # Default engine configuration
├── scripts/
│   └── generate_sample_data.py # Synthetic data generator for testing
├── main.py                     # Entry point (CLI)
├── requirements.txt
└── setup.py
```

---

## Quick Start

### 1. Install

```bash
git clone https://github.com/<your-username>/anomaly-detection-engine.git
cd anomaly-detection-engine
pip install -e ".[dev]"
```

### 2. Run with synthetic stream (easiest)

```bash
python main.py --anomaly-prob 0.1
```

The engine will start generating metric points across Kenya, Uganda, and Rwanda with a 10% anomaly injection rate. Watch the terminal for `[ALERT:HIGH]` and `[ALERT:MEDIUM]` lines.

### 3. Run with file queue stream

```bash
# Generate synthetic data files
python scripts/generate_sample_data.py --days 30 --out data/incoming

# Start engine watching that directory
python main.py --stream file --watch-dir data/incoming
```

### 4. Run tests

```bash
pytest tests/ -v
```

### 5. Custom config

```bash
cp configs/default.yaml configs/my_config.yaml
# Edit thresholds, workers, cooldown
python main.py --config configs/my_config.yaml
```

### CLI options

| Flag | Default | Description |
|---|---|---|
| `--config PATH` | built-in defaults | YAML config file |
| `--stream {mock,file}` | `mock` | Synthetic stream or JSONL file watcher |
| `--watch-dir DIR` | `data/incoming` | Directory watched when `--stream file` |
| `--anomaly-prob P` | `0.05` | Anomaly injection rate (mock stream only) |
| `--workers N` | from config | Override `max_workers` |

---

## Configuration

Edit `configs/default.yaml`:

```yaml
max_workers: 8                 # Thread pool size
baseline_window_days: 30       # Days of history per dimension
max_dimensions: 500            # Max unique (country, segment, metric) combos
zscore_threshold: 3.0          # Z-score alert threshold
baseline_delta_pct: 20.0       # Baseline % deviation threshold
wtd_threshold_pct: 15.0        # WTD % deviation threshold
mtd_threshold_pct: 10.0        # MTD % deviation threshold
alert_cooldown_minutes: 60.0   # Dedup cooldown window
alert_output_path: outputs/alerts.jsonl
poll_interval_sec: 2.0
```

---

## Alert Output

Alerts are written to `outputs/alerts.jsonl`. Each alert is a JSON object:

```json
{
  "detector": "zscore",
  "source": "kafka-mock",
  "country": "Kenya",
  "segment": "b2b",
  "metric": "billable_swaps",
  "value": 2400.0,
  "z_score": 4.21,
  "mean": 802.3,
  "stdev": 381.5,
  "threshold": 3.0,
  "timestamp": "2026-05-13T10:30:00",
  "severity": "high"
}
```

---

## Adding a New Detector

1. Create `anomaly_engine/detectors/my_detector.py`
2. Implement `detect(point, history, source) -> dict | None`
3. Add to `self.detectors` list in `engine.py`

Detectors must be **stateless** — all context is passed in per call.

---

## Extending to Real Kafka

Replace `KafkaMockStream` with a real Kafka consumer:

```python
from kafka import KafkaConsumer
from anomaly_engine.ingestion.stream import MetricStream

class KafkaStream(MetricStream):
    def __init__(self, name, topic, bootstrap_servers):
        super().__init__(name)
        self.consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

    def poll(self, timeout_sec=1.0):
        records = self.consumer.poll(timeout_ms=int(timeout_sec * 1000))
        return [json.loads(msg.value) for msgs in records.values() for msg in msgs]
```

---

## Tech Stack

- **Python 3.11+** — `threading`, `concurrent.futures`, `queue`, `collections.deque`
- **No external runtime dependencies** — pure stdlib + PyYAML
- **pytest** for testing

---

## License

MIT (add a `LICENSE` file before publishing)
