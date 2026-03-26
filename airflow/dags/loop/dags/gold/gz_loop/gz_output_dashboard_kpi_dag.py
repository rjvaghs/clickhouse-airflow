from airflow.decorators import dag, task
from airflow.models import Variable
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
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
current_dir = Path(__file__).resolve().parent
sys.path.append(str(common_util_path))
sys.path.append(str(utils_path))
sys.path.append(str(dag_path))

from loop.utils.load_config import ConfigLoader
from utils.exception_handler import push_failure
from utils.alerting import failure_alert_task

ENV = os.getenv("ENVIRONMENT")

ALERT_EMAILS = Variable.get("loop_email_group", deserialize_json=True)

default_args = {
    "owner": "airflow-loop",
    "depends_on_past": False,
    "retries": 2,
    "email": ALERT_EMAILS,
}

dag_id = "gz_output_dashboard_kpi_dag"

SQL_TEMPLATE_DIR = os.path.join(current_dir, "sql")
jinja_env = Environment(loader=FileSystemLoader(SQL_TEMPLATE_DIR))


@dag(
    dag_id=dag_id,
    schedule=None,
    max_active_runs=1,
    catchup=False,
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    tags=["clickhouse", "silver", "ramco", "transformation"],
)
def gz_output_dashboard_kpi_dag():
    cfg_loader = ConfigLoader()
    env_cfg = cfg_loader.load_env_config()
    clickhouse_conn_id = env_cfg["clickhouse"]["conn_id"]
    env = os.getenv("ENVIRONMENT")

    target_db = "gz_loop"
    silver_db = "silver"
    table_cfg_silver = cfg_loader.load_table_config(silver_db)

    source_table_fc_productivity = table_cfg_silver["tables"]["fc_productivity"]
    source_table_vehicle_productivity = table_cfg_silver["tables"]["vehicle_productivity"]
    source_table_resource_productivity_pc = table_cfg_silver["tables"]["resource_productivity"]

    @task()
    def get_affected_log_dates():
        try:
            from airflow.operators.python import get_current_context

            hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)

            # Read delta watermarks from orchestrator (required)
            ctx = get_current_context()
            dag_run = ctx.get("dag_run")
            delta_info = {}
            if dag_run and dag_run.conf:
                delta_info = dag_run.conf.get("delta_info", {}) or {}
            delta_watermarks = delta_info.get("watermarks") or {}

            if not delta_watermarks:
                logging.info("No delta_info.watermarks found in dag_run.conf; nothing to process.")
                return []

            queries = [
                (
                    f"""
                    SELECT DISTINCT log_date
                    FROM {silver_db}.{source_table_fc_productivity}
                    WHERE created_ts > toDateTime('%(wm)s')
                    """,
                    "watermark_fc_productivity",
                ),
                (
                    f"""
                    SELECT DISTINCT log_date
                    FROM {silver_db}.{source_table_vehicle_productivity}
                    WHERE created_ts > toDateTime('%(wm)s')
                    """,
                    "watermark_vehicle_productivity",
                ),
                (
                    f"""
                    SELECT DISTINCT log_date
                    FROM (
                        SELECT
                            toDate(month_year) AS month_start,
                            addDays(addMonths(toDate(month_year), 1), -1) AS month_end,
                            arrayJoin(
                                range(
                                    toUInt32(
                                        dateDiff('day', toDate(month_year), addDays(addMonths(toDate(month_year), 1), -1)) + 1
                                    )
                                )
                            ) AS day_offset,
                            addDays(toDate(month_year), day_offset) AS log_date
                        FROM {silver_db}.{source_table_resource_productivity_pc}
                        WHERE created_ts > toDateTime('%(wm)s')
                    )
                    """,
                    "watermark_resource_productivity_pc",
                ),
            ]

            affected_dates = set()

            for sql, wm_key in queries:
                if wm_key not in delta_watermarks:
                    continue
                wm_value = delta_watermarks[wm_key]
                sql_with_wm = sql.replace("%(wm)s", wm_value)
                rows = hook.execute(sql_with_wm)
                if not rows:
                    continue
                for (log_date_val,) in rows:
                    affected_dates.add(log_date_val.strftime("%Y-%m-%d"))

            result = sorted(list(affected_dates))
            logging.info(f"Output dashboard affected log_dates: {result}")
            return result
        except Exception as ex:
            push_failure(error=ex)
            raise

    @task()
    def insert_delta_gz_output_dashboard_kpi(log_dates):
        if not log_dates:
            logging.info("No affected log_dates for gz_output_dashboard_kpi.")
            return

        try:
            clickhouse_conn_id_local = cfg_loader.load_env_config()["clickhouse"]["conn_id"]

            target_table_key = "output_dashboard"
            cfg_target = cfg_loader.load_table_config(target_db)
            target_table = cfg_target["tables"][target_table_key]
            target_cluster = cfg_loader.load_env_config()["clickhouse"]["cluster"]

            source_db = silver_db
            cfg_source = table_cfg_silver
            source_table_fc_productivity_local = cfg_source["tables"]["fc_productivity"]
            source_table_vehicle_productivity_local = cfg_source["tables"]["vehicle_productivity"]
            source_table_region = cfg_source["tables"]["region"]
            source_table_resource_productivity_pc_local = cfg_source["tables"]["resource_productivity"]

            hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id_local)

            template = jinja_env.get_template("gz_output_dashboard_kpi.sql.jinja")

            sql = template.render(
                target_db=target_db,
                target_table=target_table,
                source_db=source_db,
                source_table_fc_productivity=source_table_fc_productivity_local,
                source_table_vehicle_productivity=source_table_vehicle_productivity_local,
                source_table_region=source_table_region,
                source_table_resource_productivity_pc=source_table_resource_productivity_pc_local,
                target_cluster=target_cluster,
                log_dates=log_dates,
            )

            logging.info(f"Executing rendered SQL for {dag_id}")
            try:
                hook.execute(sql)
                logging.info(f"Successfully inserted delta into table {target_table} in {target_db}")
            except Exception as ex:
                logging.error(f"Failed to insert delta into table {target_table} in {target_db}: {ex}")
                raise
        except Exception as ex:
            push_failure(error=ex)
            raise

    affected_log_dates = get_affected_log_dates()
    insert = insert_delta_gz_output_dashboard_kpi(affected_log_dates)

    email_variable = "loop_email_config"
    error_variable = "loop_error_config"
    email_config = Variable.get(email_variable, deserialize_json=True)
    is_error_insert = int(Variable.get(error_variable, deserialize_json=False))
    is_alert = int(email_config[dag_id])
    

    if is_error_insert == 1:
        failure_alert = failure_alert_task(
            alert_emails=ALERT_EMAILS,
            clickhouse_conn_id=clickhouse_conn_id,
            is_alert=is_alert,
        )()

        affected_log_dates >> insert >> failure_alert
    else:
        affected_log_dates >> insert


gz_output_dashboard_kpi_dag = gz_output_dashboard_kpi_dag()