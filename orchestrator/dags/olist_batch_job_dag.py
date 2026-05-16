from datetime import datetime, timedelta
import os
import json
import shutil
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
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
    def summarize_spark_metrics(ti, **_kwargs):
        raw = ti.xcom_pull(task_ids="run_spark_batch")
        if not raw:
            print("No XCom output from run_spark_batch.")
            return

        metrics_line = None
        for line in str(raw).splitlines():
            if line.startswith("METRICS_JSON="):
                metrics_line = line.split("=", 1)[1].strip()
                break

        if not metrics_line:
            print("Spark metrics marker not found in task output.")
            return

        metrics = json.loads(metrics_line)
        print("=== Spark Metrics Summary ===")
        for k in sorted(metrics.keys()):
            print(f"{k}={metrics[k]}")

    def cleanup_airflow_logs_fn(**_kwargs):
        keep = int(os.getenv("AIRFLOW_LOG_KEEP_RUNS", "10"))
        base = Path("/opt/airflow/logs")
        if not base.exists():
            print("cleanup_airflow_logs: log base path not found, skipping")
            return

        for dag_dir in sorted(base.glob("dag_id=*")):
            if not dag_dir.is_dir():
                continue
            run_dirs = sorted([p for p in dag_dir.glob("run_id=*") if p.is_dir()])
            if len(run_dirs) <= keep:
                continue
            to_remove = run_dirs[: len(run_dirs) - keep]
            for p in to_remove:
                shutil.rmtree(p, ignore_errors=True)
                print(f"cleanup_airflow_logs: removed {p}")

        # prune empty directories after run cleanup
        for p in sorted(base.rglob("*"), reverse=True):
            if p.is_dir():
                try:
                    p.rmdir()
                except OSError:
                    pass

    start = EmptyOperator(task_id="start")

    run_warehouse_bootstrap = DockerOperator(
        task_id="run_warehouse_bootstrap",
        image="olist-warehouse-bootstrap:latest",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="ingest_olist-net",
        mount_tmp_dir=False,
        environment={
            "PGHOST": os.getenv("PGHOST", "postgres"),
            "PGPORT": os.getenv("PGPORT", "5432"),
            "PGUSER": os.getenv("PG_USER", "postgres"),
            "PGPASSWORD": os.getenv("PG_PASSWORD", "postgres"),
            "PGDATABASE": os.getenv("PGDATABASE", "postgres"),
            "INCLUDE_DEV_SQL": os.getenv("INCLUDE_DEV_SQL", "1"),
        },
    )

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
            "BOOTSTRAP_TARGET_RUNS": os.getenv("BOOTSTRAP_TARGET_RUNS", "5"),
            "BOOTSTRAP_MIN_BATCH": os.getenv("BOOTSTRAP_MIN_BATCH", "10000"),
            "BOOTSTRAP_MAX_BATCH": os.getenv("BOOTSTRAP_MAX_BATCH", "200000"),
            "PYSPARK_SUBMIT_ARGS": "--conf spark.ui.showConsoleProgress=false pyspark-shell"
        },
        do_xcom_push=True,
    )

    summarize_metrics = PythonOperator(
        task_id="summarize_metrics",
        python_callable=summarize_spark_metrics,
    )

    cleanup_airflow_logs = PythonOperator(
        task_id="cleanup_airflow_logs",
        python_callable=cleanup_airflow_logs_fn,
    )

    end = EmptyOperator(task_id="end")

    start >> run_warehouse_bootstrap >> run_spark_batch >> summarize_metrics >> cleanup_airflow_logs >> end
