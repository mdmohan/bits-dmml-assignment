from pathlib import Path
import yaml


def load_topics_config(path: str | None = None) -> dict:
    cfg_path = Path(path or Path(__file__).resolve().parents[1] / "config" / "topics.yaml")
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def minio_topic_path(cfg: dict, topic_name: str) -> str:
    bucket = cfg["minio"]["bucket"]
    base_prefix = cfg["minio"]["base_prefix"]
    date_glob = cfg["minio"]["date_glob"]
    hour_glob = cfg["minio"]["hour_glob"]
    topic = cfg["topics"][topic_name]
    # s3a://<bucket>/datalake/raw/topic=<topic>/dt=*/hour=*/*.json
    return f"s3a://{bucket}/{base_prefix}/topic={topic}/{date_glob}/{hour_glob}/*.json*"
