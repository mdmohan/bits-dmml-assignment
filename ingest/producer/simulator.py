"""
simulator.py  –  Olist event replay service
============================================
Runs as a long-lived service.  Stop with Ctrl-C or SIGTERM.

Usage
-----
    python simulator.py [--config path/to/config.yaml]

Traffic model
-------------
Inter-event delays are drawn from a Poisson process (exponential distribution)
so arrival times are naturally bursty.  Three additional layers add realism:

  1. Time-of-day scaling  – based on the hour embedded in each replayed event_ts.
     Events that originally happened during peak shopping hours are replayed
     faster; overnight events are replayed slower.

  2. Burst mode  – with probability `burst_prob` the simulator enters a short
     burst and sends `burst_size` events in rapid succession (10–80 ms apart),
     mimicking flash-sales or checkout surges.

  3. Slow-spell mode  – with probability `slow_prob` the simulator enters a
     quiet spell and adds an extra 1–4 s pause, mimicking off-peak lulls.
"""
import argparse
import logging
import random
import signal
import sys
import time
from pathlib import Path

import yaml

# ── Allow running from either ingest/ or ingest/producer/ ────────────────────
sys.path.insert(0, str(Path(__file__).parent))

from event_builder import build_envelope
from olist_loader import build_event_timeline, load_all


class _ConsoleProducer:
    """Drop-in replacement for OlistKafkaProducer that prints to stdout."""

    def publish(self, topic: str, key: str, value: dict) -> None:
        import json
        print(f"[{topic}] key={key} | {json.dumps(value, default=str)}")

    def flush(self, timeout: float = 15.0) -> None:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("simulator")

# ── Graceful shutdown flag ────────────────────────────────────────────────────
_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    logger.info("Signal %s received – shutting down after current event…", sig)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ── Traffic profile ───────────────────────────────────────────────────────────
# Hour-of-day multiplier (index = hour 0-23).
# Higher value → more events per second at that hour.
_HOURLY_MULT = [
    0.15, 0.10, 0.08, 0.08, 0.10, 0.20,   # 00-05  overnight
    0.45, 0.75, 1.10, 1.40, 1.60, 1.80,   # 06-11  morning ramp
    1.55, 1.45, 1.55, 1.65, 1.85, 2.00,   # 12-17  afternoon peak
    1.80, 1.50, 1.20, 0.90, 0.60, 0.30,   # 18-23  evening wind-down
]


class TrafficProfile:
    """Generates realistic variable inter-event delays."""

    def __init__(self, base_rate: float, burst_prob: float,
                 burst_size: int, slow_prob: float):
        self._base_rate    = base_rate    # mean events/sec at multiplier=1.0
        self._burst_prob   = burst_prob   # P(enter burst mode each event)
        self._burst_size   = burst_size   # events sent during a burst
        self._slow_prob    = slow_prob    # P(enter slow-spell each event)
        self._burst_left   = 0

    def next_delay(self, simulated_hour: int) -> float:
        """Return seconds to wait before publishing the next event."""

        # ── burst mode: very fast back-to-back events ─────────────────────────
        if self._burst_left > 0:
            self._burst_left -= 1
            return random.uniform(0.01, 0.08)

        # ── maybe enter burst ─────────────────────────────────────────────────
        if random.random() < self._burst_prob:
            self._burst_left = random.randint(3, self._burst_size)
            return random.uniform(0.01, 0.05)

        # ── maybe enter slow spell ────────────────────────────────────────────
        if random.random() < self._slow_prob:
            return random.uniform(1.0, 4.0)

        # ── normal Poisson arrival with time-of-day scaling ───────────────────
        hour_mult     = _HOURLY_MULT[simulated_hour % 24]
        effective_rate = max(self._base_rate * hour_mult, 0.05)
        delay = random.expovariate(effective_rate)
        return max(0.05, min(delay, 8.0))   # clamp: 50 ms … 8 s


# ── Main loop ─────────────────────────────────────────────────────────────────
def run(cfg: dict, dry_run: bool = False) -> None:
    sim_cfg   = cfg["simulator"]
    kafka_cfg = cfg["kafka"]

    data_dir   = sim_cfg["data_dir"]
    loop       = sim_cfg.get("loop", True)
    log_every  = sim_cfg.get("log_every_n", 100)

    profile = TrafficProfile(
        base_rate  = sim_cfg.get("base_rate_per_sec", 2.0),
        burst_prob = sim_cfg.get("burst_prob", 0.05),
        burst_size = sim_cfg.get("burst_size", 15),
        slow_prob  = sim_cfg.get("slow_prob", 0.03),
    )

    if dry_run:
        logger.info("DRY-RUN mode – events will be printed to console, not sent to Kafka.")
        producer = _ConsoleProducer()
    else:
        from kafka_producer import OlistKafkaProducer
        producer = OlistKafkaProducer(kafka_cfg["bootstrap_servers"])

    logger.info("Loading Olist data from %s …", data_dir)
    dfs      = load_all(data_dir)
    timeline = build_event_timeline(dfs)
    total    = len(timeline)
    logger.info("Timeline ready: %d events.  Starting replay (loop=%s)…", total, loop)

    run_number = 0
    published  = 0
    errors     = 0

    while not _shutdown:
        run_number += 1
        logger.info("── Replay run #%d (%d events) ──", run_number, total)

        for idx, row in timeline.iterrows():
            if _shutdown:
                break

            try:
                topic, key, message = build_envelope(row.to_dict())
                producer.publish(topic, key, message)
                published += 1
            except Exception as exc:
                errors += 1
                logger.error("Failed to publish event %d: %s", idx, exc)

            if published % log_every == 0:
                logger.info(
                    "Progress: published=%d errors=%d run=%d",
                    published, errors, run_number,
                )

            simulated_hour = row["event_ts"].hour if hasattr(row["event_ts"], "hour") else 12
            delay = profile.next_delay(simulated_hour)
            time.sleep(delay)

        if not loop:
            logger.info("All events replayed once. loop=false – exiting.")
            break

    logger.info(
        "Shutting down. Total published=%d errors=%d across %d run(s).",
        published, errors, run_number,
    )
    producer.flush()
    logger.info("Producer flushed. Goodbye.")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Olist Kafka event simulator")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent.parent / "config.yaml"),
        help="Path to config.yaml (default: ../config.yaml relative to this file)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print events to console instead of publishing to Kafka.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        logger.error("Config file not found: %s", config_path)
        sys.exit(1)

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    run(cfg, dry_run=args.dry_run)
