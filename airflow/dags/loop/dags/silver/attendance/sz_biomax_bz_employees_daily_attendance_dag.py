from airflow.decorators import dag, task
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import get_current_context
from airflow.models import Variable
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
import os
from pathlib import Path
import sys
import logging

# --- Setup Paths ---
common_util_path = Path(__file__).resolve().parents[6]
utils_path = Path(__file__).resolve().parents[4]
dag_path = Path(__file__).resolve().parents[2]
sys.path.append(str(common_util_path))
sys.path.append(str(utils_path))
sys.path.append(str(dag_path))
current_dir = Path(__file__).resolve().parent

from loop.utils.load_config import ConfigLoader
from utils.exception_handler import push_failure
from utils.alerting import failure_alert_task

# ------------------------------------------------------------------
# DAG Config
# ------------------------------------------------------------------
ENV = os.getenv("ENVIRONMENT")

ALERT_EMAILS = Variable.get("loop_email_group", deserialize_json=True)

dag_id = "sz_biomax_bz_employee_daily_attendance_dag"

default_args = {
    "owner": "airflow-loop",
    "depends_on_past": False,
    "retries": 1,
    "email": ALERT_EMAILS,
}

# ------------------------------------------------------------------
# DAG Definition
# ------------------------------------------------------------------
@dag(
    dag_id=dag_id,
    schedule=None,
    catchup=False,
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    params={
        "event_date_from": None,
        "event_date_to": None,
    },
    tags=["clickhouse", "silver", "biomax", "attendance"],
)
def bz_to_sz_biomax_employee_daily_attendance_transformation():

    cfg_loader = ConfigLoader()
    env_cfg = cfg_loader.load_env_config()
    clickhouse_conn_id = env_cfg["clickhouse"]["conn_id"]

    # --------------------------------------------------------------
    # Read date range
    # --------------------------------------------------------------
    @task
    def read_event_date_range():
        ctx = get_current_context()
        ti = ctx["ti"]
        dag_run = ctx.get("dag_run")
        params = ctx.get("params") or {}
        conf = dag_run.conf if dag_run else {}

        event_date_from = conf.get("event_date_from") or params.get("event_date_from")
        event_date_to = conf.get("event_date_to") or params.get("event_date_to")

        if not event_date_from or not event_date_to:
            push_failure(
                error="event_date_from and event_date_to must be provided",
                error_type="ValueError",
                include_traceback=False,
            )
            raise ValueError("event_date_from and event_date_to must be provided")

        return {
            "event_date_from": event_date_from,
            "event_date_to": event_date_to,
        }

    # --------------------------------------------------------------
    # Transform Bronze → Silver
    # --------------------------------------------------------------
    @task()
    def transform_to_silver(date_range: dict):
        ctx = get_current_context()

        try:
            bronze_cfg = cfg_loader.load_table_config("bronze")
            silver_cfg = cfg_loader.load_table_config("silver")

            bronze_source_table = bronze_cfg["tables"]["employee_daily_attendance"]
            silver_source_table = silver_cfg["tables"]["employee"]
            silver_target_table = silver_cfg["tables"]["employee_daily_attendance"]

            hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)

            SQL_TEMPLATE_DIR = os.path.join(current_dir, "sql")
            jinja_env = Environment(loader=FileSystemLoader(SQL_TEMPLATE_DIR))
            template = jinja_env.get_template(
                "sz_biomax_bz_employees_daily_attendance.sql.jinja"
            )

            rendered_sql = template.render(
                dag_id=dag_id,
                bronze_db="bronze",
                silver_db="silver",
                bronze_source_table=bronze_source_table,
                silver_source_table=silver_source_table,
                silver_target_table=silver_target_table,
                event_date_from=date_range["event_date_from"],
                event_date_to=date_range["event_date_to"],
            )

            logging.info(f"Executing Silver SQL:\n{rendered_sql}")
            hook.execute(rendered_sql)

        except Exception as ex:
            logging.error(f"Silver transformation failed: {ex}")
            push_failure(error=ex)
            raise

    # --------------------------------------------------------------
    # DAG Flow
    # --------------------------------------------------------------
    date_range = read_event_date_range()
    silver = transform_to_silver(date_range)

    # --------------------------------------------------------------
    # Environment-Based Alert Control
    # --------------------------------------------------------------
    email_variable = "loop_email_config"
    error_variable = "loop_error_config"
    email_config = Variable.get(email_variable, deserialize_json=True)
    is_error_insert = int(Variable.get(error_variable, deserialize_json=False))
    is_alert = int(email_config[dag_id])
    

    if is_error_insert == 1:

        failure_alert = failure_alert_task(
            alert_emails=ALERT_EMAILS,
            clickhouse_conn_id=clickhouse_conn_id,
            is_alert=is_alert
        )()

        date_range >> silver

        [date_range, silver] >> failure_alert

    else:
        date_range >> silver


bz_to_sz_biomax_employee_daily_attendance_transformation = bz_to_sz_biomax_employee_daily_attendance_transformation()