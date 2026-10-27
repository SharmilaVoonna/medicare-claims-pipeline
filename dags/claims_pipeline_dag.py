r"""Airflow DAG: Medicare claims pipeline.

check_for_new_files --(new zips)--> ingest --+
                    \--(none)----> skip_ingest --+--> transform --> load_warehouse --> quality_checks

If new zip files are in data/landing, they are ingested first. If not, the
pipeline re-processes the raw data already on disk. Retries and a failure
callback are configured for every task.
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

PROJECT = "/opt/project"
LANDING = Path(PROJECT) / "data" / "landing"


def notify_failure(context):
    # Free alerting: write to the task log. Swap for email/Slack later.
    ti = context["task_instance"]
    print(f"ALERT: task {ti.task_id} failed in run {context['run_id']}. Log: {ti.log_url}")


def choose_path():
    return "ingest" if any(LANDING.glob("*.zip")) else "skip_ingest"


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="medicare_claims_pipeline",
    description="CMS DE-SynPUF claims: ingest -> PySpark -> DuckDB star schema -> DQ checks",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["claims", "portfolio"],
) as dag:

    run = f"cd {PROJECT}/scripts && python"

    check_for_new_files = BranchPythonOperator(
        task_id="check_for_new_files", python_callable=choose_path)

    ingest = BashOperator(task_id="ingest", bash_command=f"{run} ingest.py")
    skip_ingest = EmptyOperator(task_id="skip_ingest")

    transform = BashOperator(
        task_id="transform",
        bash_command=f"{run} transform.py",
        trigger_rule="none_failed_min_one_success",  # runs after either branch
    )
    load_warehouse = BashOperator(task_id="load_warehouse", bash_command=f"{run} load_warehouse.py")
    quality_checks = BashOperator(task_id="quality_checks", bash_command=f"{run} quality_checks.py")

    check_for_new_files >> [ingest, skip_ingest] >> transform >> load_warehouse >> quality_checks
