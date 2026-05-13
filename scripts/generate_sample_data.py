"""
generate_sample_data.py — Generate synthetic JSONL metric files for FileQueueStream testing.

Usage:
    python scripts/generate_sample_data.py --days 30 --out data/incoming
"""

import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path


COUNTRIES = ["Kenya", "Uganda", "Rwanda"]
SEGMENTS = ["b2b", "b2c"]
METRICS = {
    "billable_swaps": (800, 60),
    "active_bikes": (3000, 200),
    "dormant_bikes": (400, 50),
    "reactivation_rate": (0.12, 0.015),
}
ANOMALY_DAYS = {5, 12, 20}  # Inject anomalies on these day offsets


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--out", type=str, default="data/incoming")
    parser.add_argument("--anomaly-prob", type=float, default=0.0,
                        help="Probability of anomaly per point (0=only fixed days)")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_date = datetime(2026, 4, 1)
    lines = []

    for day_offset in range(args.days):
        date = base_date + timedelta(days=day_offset)
        for country in COUNTRIES:
            for segment in SEGMENTS:
                for metric, (mean, stdev) in METRICS.items():
                    is_anomaly = day_offset in ANOMALY_DAYS or random.random() < args.anomaly_prob
                    if is_anomaly:
                        value = mean * random.choice([0.35, 2.5, 3.0])
                    else:
                        value = max(0, random.gauss(mean, stdev * 0.4))

                    lines.append({
                        "country": country,
                        "segment": segment,
                        "metric": metric,
                        "value": round(value, 4),
                        "timestamp": date.isoformat(),
                        "weekday": date.weekday(),
                        "day_of_month": date.day,
                        "source": "synthetic",
                    })

    out_file = out_dir / f"synthetic_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"
    with open(out_file, "w") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")

    print(f"Written {len(lines)} metric points to {out_file}")


if __name__ == "__main__":
    main()
