"""
simulator.py  –  Olist event replay service
============================================
Replays the Olist event timeline using the actual timestamps from the CSV data.
The `scale` config controls replay speed:
    scale = 1      → real-time (original gaps between events)
    scale = 60     → 60x faster (1 hour of data replays in 1 minute)
    scale = 3600   → 3600x faster (1 hour of data replays in 1 second)

Stop with Ctrl-C or SIGTERM.

Usage
-----
    python simulator.py [--config path/to/config.yaml] [--dry-run]
"""
import argparse
import logging
import signal
import sys
import time
from pathlib import Path

import yaml

# ── Allow running from either ingest/ or ingest/producer/ ────────────────────
sys.path.insert(0, str(Path(__file__).parent))

from event_builder import build_envelope
from olist_loader import build_event_timeline, load_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("simulator")

# ── Graceful shutdown ─────────────────────────────────────────────────────────
_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    logger.info("Signal %s received – shutting down after current event…", sig)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ── Console producer stub ─────────────────────────────────────────────────────
class _ConsoleProducer:
    """Drop-in replacement that prints events to stdout."""

    def publish(self, topic: str, key: str, value: dict) -> None:
        import json
        print(f"[{topic}] key={key} | {json.dumps(value, default=str)}")

    def flush(self, timeout: float = 15.0) -> None:
        pass


# ── Main loop ─────────────────────────────────────────────────────────────────
def run(cfg: dict, dry_run: bool = False) -> None:
    sim_cfg = cfg["simulator"]
    kafka_cfg = cfg["kafka"]

    data_dir = sim_cfg["data_dir"]
    scale = sim_cfg.get("scale", 1.0)
    loop = sim_cfg.get("loop", True)
    log_every = sim_cfg.get("log_every_n", 100)

    if dry_run:
        logger.info("DRY-RUN mode – printing to console, not Kafka.")
        producer = _ConsoleProducer()
    else:
        from kafka_producer import OlistKafkaProducer
        producer = OlistKafkaProducer(kafka_cfg["bootstrap_servers"])

    logger.info("Loading Olist data from %s …", data_dir)
    dfs = load_all(data_dir)
    timeline = build_event_timeline(dfs)
    total = len(timeline)
    logger.info("Timeline ready: %d events. scale=%sx, loop=%s", total, scale, loop)

    run_number = 0
    published = 0
    errors = 0

    while not _shutdown:
        run_number += 1
        logger.info("── Replay run #%d (%d events) ──", run_number, total)

        prev_ts = None

        for idx, row in timeline.iterrows():
            if _shutdown:
                break

            # ── Compute delay from timestamp gap ──────────────────────────────
            current_ts = row["event_ts"]
            if prev_ts is not None and scale > 0:
                gap_seconds = (current_ts - prev_ts).total_seconds()
                if gap_seconds > 0:
                    time.sleep(gap_seconds / scale)
            prev_ts = current_ts

            # ── Publish ───────────────────────────────────────────────────────
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
    parser = argparse.ArgumentParser(description="Olist event replay simulator")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent.parent / "config.yaml"),
        help="Path to config.yaml",
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
