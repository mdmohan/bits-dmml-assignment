from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator


default_args = {
    "owner": "dmml-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="olist_batch_job_every_20_min",
    default_args=default_args,
    description="Run Olist Spark batch job every 20 minutes",
    start_date=datetime(2026, 5, 15),
    schedule_interval="*/20 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dmml", "spark", "batch"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_spark_batch = DockerOperator(
        task_id="run_spark_batch",
        image="olist-spark-job:latest",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="ingest_olist-net",
        mount_tmp_dir=False,
        port_bindings={4040: 4040},
        environment={
            "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "minio:9000"),
            "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "MINIO_BUCKET": os.getenv("MINIO_BUCKET", "datalake"),
            "MINIO_SECURE": os.getenv("MINIO_SECURE", "false"),
            "PG_JDBC_URL": os.getenv("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/postgres"),
            "PG_USER": os.getenv("PG_USER", "postgres"),
            "PG_PASSWORD": os.getenv("PG_PASSWORD", "postgres"),
            "ENABLED_PROCESSORS": os.getenv(
                "ENABLED_PROCESSORS", "order_events,payment_events,delivery_events,review_events"
            ),
            "WINDOW_MINUTES": os.getenv("WINDOW_MINUTES", "30"),
        },
    )

    end = EmptyOperator(task_id="end")

    start >> run_spark_batch >> end
