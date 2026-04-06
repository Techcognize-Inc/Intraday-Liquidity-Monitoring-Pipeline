"""
Pending Payments DAG — Airflow
================================
Airflow is a workflow scheduler. A DAG (Directed Acyclic Graph) is
a collection of tasks that run in a defined order.

This DAG:
  - Runs every 30 minutes during business hours (Mon-Fri, 07:00-18:00 UTC)
  - Refreshes the pending_payments table in PostgreSQL
  - Cleans up old settled payment records to keep the table lean
  - Logs a summary so we can monitor it

Why Airflow for this?
  The real-time Flink engine handles payments AS THEY HAPPEN.
  This DAG handles the SCHEDULED/BATCH side: "what payments are
  expected in the next 2 hours?" — a periodic refresh job,
  which is exactly what Airflow is designed for.

Task flow:
  generate_pending_payments
          │
          ▼
  cleanup_settled_payments
          │
          ▼
  log_summary
"""

from datetime import datetime, timedelta   # timedelta used for retry_delay

from airflow import DAG                             # DAG class — defines the workflow
from airflow.operators.python import PythonOperator  # runs a Python function as a task
from airflow.operators.empty import EmptyOperator    # no-op task (useful as a placeholder)
from airflow.utils.dates import days_ago             # helper: "start from 1 day ago"

import sys
import os

# Add project root to Python path so Airflow workers can import producers module
# Airflow runs tasks in worker processes that may not have the project root in sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── DAG default arguments ─────────────────────────────────────────
# These apply to ALL tasks in the DAG unless overridden per-task
default_args = {
    "owner":            "liquidity-pipeline",   # who owns this DAG (shown in Airflow UI)
    "depends_on_past":  False,                  # don't wait for previous run to succeed
    "email_on_failure": False,                  # don't send email on failure (no email configured)
    "email_on_retry":   False,                  # don't send email on retry
    "retries":          2,                      # retry each task up to 2 times if it fails
    "retry_delay":      timedelta(minutes=2),   # wait 2 minutes between retries
}

# ── DAG definition ────────────────────────────────────────────────
# The `with DAG(...)` block defines the DAG and its configuration
# Everything indented inside is part of this DAG
with DAG(
    dag_id="pending_payments_dag",                          # unique name shown in Airflow UI
    description="Refresh pending payments queue for treasury liquidity planning",
    default_args=default_args,
    schedule_interval="*/30 7-18 * * 1-5",
    # ^ cron expression: "*/30" = every 30 minutes
    #                    "7-18" = only between hour 7 and 18 (07:00–18:00 UTC)
    #                    "* *"  = any day of month, any month
    #                    "1-5"  = Monday(1) through Friday(5) only
    start_date=days_ago(1),   # start scheduling from 1 day ago (immediate first run)
    catchup=False,            # don't backfill missed runs — we only care about current state
    tags=["liquidity", "treasury", "intraday"],   # labels for filtering in Airflow UI
) as dag:

    # ── Task 1: Generate and load pending payments ─────────────────
    def task_generate_pending_payments(**context):
        """
        Calls pending_payments_generator.run() which:
          1. Generates 50 synthetic upcoming payments
          2. Expires past-due pending payments
          3. Inserts new ones into PostgreSQL

        **context is Airflow's task context dict — contains metadata
        about the current run (execution_date, task_instance, etc.)
        """
        from producers.pending_payments_generator import run    # import here to avoid circular imports
        result = run(count=50)                                  # generate and load 50 payments
        context["ti"].xcom_push(key="load_result", value=result)
        # xcom_push stores the result dict in Airflow's XCom system
        # "ti" = task instance, "load_result" = the key we'll use to retrieve it later
        return result

    generate_task = PythonOperator(
        task_id="generate_pending_payments",          # unique ID for this task in the DAG
        python_callable=task_generate_pending_payments,   # function to run
        provide_context=True,                         # pass Airflow context dict as **context
    )

    # ── Task 2: Cleanup old settled/cancelled records ─────────────
    def task_cleanup_old_payments(**context):
        """
        Deletes SETTLED, CANCELLED, and FAILED payment records older than 24 hours.
        This prevents the pending_payments table from growing forever.
        Grafana queries this table — keeping it lean means faster queries.
        """
        import psycopg2   # import inside function — only needed when this task runs
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST",     "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB",     "liquidity"),
            user=os.getenv("POSTGRES_USER",     "liquidity_user"),
            password=os.getenv("POSTGRES_PASSWORD", "liquidity_pass"),
        )
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM pending_payments
                    WHERE status IN ('SETTLED', 'CANCELLED', 'FAILED')   -- only completed payments
                      AND created_at < NOW() - INTERVAL '24 hours'       -- older than 24 hours
                """)
                deleted = cur.rowcount   # number of rows deleted
            conn.commit()   # commit the DELETE
        finally:
            conn.close()

        print(f"Cleaned up {deleted} old settled/cancelled payment records")
        context["ti"].xcom_push(key="deleted_count", value=deleted)   # pass count to summary task
        return deleted

    cleanup_task = PythonOperator(
        task_id="cleanup_settled_payments",
        python_callable=task_cleanup_old_payments,
        provide_context=True,
    )

    # ── Task 3: Log summary ────────────────────────────────────────
    def task_log_summary(**context):
        """
        Reads results from the previous two tasks via XCom and logs a summary.
        XCom (Cross-Communication) is Airflow's mechanism for tasks to share data.
        xcom_pull() retrieves a value that was stored by xcom_push() in another task.
        """
        ti = context["ti"]   # task instance — used to interact with XCom
        load_result   = ti.xcom_pull(task_ids="generate_pending_payments", key="load_result")
        # ^ retrieve the dict pushed by generate_task: {"generated": 50, "inserted": 47}
        deleted_count = ti.xcom_pull(task_ids="cleanup_settled_payments",  key="deleted_count")
        # ^ retrieve the int pushed by cleanup_task

        print(
            f"Pending Payments DAG Summary | "
            f"Generated={load_result.get('generated', '?')} | "    # how many we tried to generate
            f"Inserted={load_result.get('inserted', '?')} | "      # how many were actually new
            f"Cleaned up={deleted_count}"                           # how many old records were deleted
        )

    summary_task = PythonOperator(
        task_id="log_summary",
        python_callable=task_log_summary,
        provide_context=True,
    )

    # ── Task dependency definition ─────────────────────────────────
    # The >> operator sets execution order (like an arrow: A runs before B)
    # generate → cleanup → summary
    # This means: generate runs first, then cleanup, then summary
    generate_task >> cleanup_task >> summary_task
