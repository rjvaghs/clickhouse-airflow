import json
import socket
import logging
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context
from airflow.utils.email import send_email_smtp
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models import Variable


def failure_alert_task(
    *,
    alert_emails: list[str],
    clickhouse_conn_id: str,
    is_alert: int
):
    """
    Returns a TaskFlow task that:
    - Collects failure_info XComs
    - Inserts into bronze.error_log (ALWAYS)
    - Sends email alert (only if enabled in Airflow Variable `email-config`)
    """

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def failure_alert():
        ctx = get_current_context()
        dag = ctx["dag"]
        dag_run = ctx["dag_run"]
        ti = ctx["ti"]

        failures = []

        for upstream_task_id in ctx["task"].upstream_task_ids:
            failure = ti.xcom_pull(
                task_ids=upstream_task_id,
                key="failure_info",
            )
            if failure:
                failures.append(failure)

        if not failures:
            logging.warning("Alert triggered but no failure XComs found")
            return

        # ---------------------------------------------------------
        # Always insert into error table
        # ---------------------------------------------------------
        rows = []
        for f in failures:
            rows.append((
                dag.dag_id,
                f["task_id"],
                dag_run.run_id,
                dag_run.logical_date,
                f["try_number"],
                f["state"],
                f["error_type"],
                f["error_message"],
                f["log_url"],
                socket.gethostname(),
                dag.owner or "airflow",
                ",".join(dag.default_args.get("email", [])),
                json.dumps({
                    "traceback": f["traceback"],
                    "params": ctx.get("params"),
                }),
            ))

        try:
            hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)
            hook.execute(
                """
                INSERT INTO audit.error_log
                (
                    dag_id,
                    task_id,
                    run_id,
                    execution_date,
                    try_number,
                    state,
                    error_type,
                    error_message,
                    log_url,
                    host,
                    owner,
                    email,
                    extra
                )
                VALUES
                """,
                rows,
            )
            logging.info("Error log inserted successfully.")
        except Exception as ex:
            logging.error("Error log insert failed: %s", ex)


        if is_alert == 1:
            try:
                # ---------------------------------------------------------
                # Send Email (only if enabled)
                # ---------------------------------------------------------
                send_email_smtp(
                    to=alert_emails,
                    subject=f"[AIRFLOW FAILURE] {dag.dag_id}",
                    html_content=f"""
                    <h3>DAG Failure</h3>
                    <b>DAG:</b> {dag.dag_id}<br>
                    <b>Run ID:</b> {dag_run.run_id}<br>
                    <b>Execution Date:</b> {dag_run.logical_date}<br><br>
                    <pre>{json.dumps(failures, indent=2)}</pre>
                    """,
                )
                logging.info("Failure email sent.")
            except Exception as ex:
                logging.error("Failed to send email alerts: %s", ex)
        else:
            logging.info(
                "Email alerts disabled for DAG %s via Airflow Variable.",
                dag.dag_id,
            )
            return
            

    return failure_alert