from airflow.decorators import dag, task
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from typing import Any, Dict, Optional
import os
from pathlib import Path
import sys
import logging

from airflow.models import DagBag, Variable
from airflow.utils import timezone

common_util_path = Path(__file__).resolve().parents[6]
utils_path = Path(__file__).resolve().parents[4]
dag_path = Path(__file__).resolve().parents[2]
sys.path.append(str(common_util_path))
sys.path.append(str(utils_path))
sys.path.append(str(dag_path))

from loop.utils.load_config import ConfigLoader
from utils.exception_handler import push_failure
from utils.alerting import failure_alert_task
from clickhouse_watermark_sensor import ClickHouseWatermarkSensor


dag_id = "gz_kpi_delta_orchestrator_dag"

ALERT_EMAILS = Variable.get("loop_email_group", deserialize_json=True)

default_args = {
    "owner": "airflow-loop",
    "depends_on_past": False,
    "retries": 1,
    "email": ALERT_EMAILS,
}


@dag(
    dag_id=dag_id,
    schedule="*/2 * * * *",  # check every 2 minutes
    max_active_runs=1,
    catchup=False,
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    tags=["clickhouse", "gz_loop", "delta-orchestrator"],
)
def gz_kpi_delta_orchestrator_dag():
    """
    Central deferrable DAG that watches silver tables and triggers
    downstream KPI DAGs when their base tables receive new data.
    """

    cfg_loader = ConfigLoader()
    env_cfg = cfg_loader.load_env_config()
    clickhouse_conn_id = env_cfg["clickhouse"]["conn_id"]

    silver_db = "silver"
    table_cfg_silver = cfg_loader.load_table_config(silver_db)
    table_cfg_audit = cfg_loader.load_table_config("audit")

    watermark_table = table_cfg_audit["tables"]["dag_watermarks"]

    source_table_employee_attendance = table_cfg_silver["tables"]["employee_daily_attendance"]
    source_table_da_productivity = table_cfg_silver["tables"]["da_productivity"]
    source_table_shopper_productivity = table_cfg_silver["tables"]["shopper_productivity"]
    source_table_vehicle_productivity = table_cfg_silver["tables"]["vehicle_productivity"]
    source_table_resource_productivity_pc = table_cfg_silver["tables"]["resource_productivity"]
    source_table_resource_level_productivity = table_cfg_silver["tables"]["resource_level_productivity"]
    source_table_fc_productivity = table_cfg_silver["tables"]["fc_productivity"]

    sources = [
        {
            "database": silver_db,
            "table": source_table_employee_attendance,
            "timestamp_column": "created_ts",
            "watermark_column_name": "watermark_employee_daily_attendance",
        },
        {
            "database": silver_db,
            "table": source_table_da_productivity,
            "timestamp_column": "created_ts",
            "watermark_column_name": "watermark_da_productivity",
        },
        {
            "database": silver_db,
            "table": source_table_shopper_productivity,
            "timestamp_column": "created_ts",
            "watermark_column_name": "watermark_shopper_productivity",
        },
        {
            "database": silver_db,
            "table": source_table_vehicle_productivity,
            "timestamp_column": "created_ts",
            "watermark_column_name": "watermark_vehicle_productivity",
        },
        {
            "database": silver_db,
            "table": source_table_fc_productivity,
            "timestamp_column": "created_ts",
            "watermark_column_name": "watermark_fc_productivity",
        },
        {
            "database": silver_db,
            "table": source_table_resource_productivity_pc,
            "timestamp_column": "created_ts",
            "watermark_column_name": "watermark_resource_productivity_pc",
        },
        {
            "database": silver_db,
            "table": source_table_resource_level_productivity,
            "timestamp_column": "event_ts",
            "watermark_column_name": "watermark_resource_level_productivity",
        },
    ]

    wait_for_any_delta = ClickHouseWatermarkSensor(
        task_id="wait_for_any_kpi_delta",
        clickhouse_conn_id=clickhouse_conn_id,
        watermark_db="audit",
        watermark_table=watermark_table,
        dag_id=dag_id,
        sources=sources,
        poke_interval=120,
        timeout=4 * 60 * 60,
    )

    @task()
    def decide_and_prepare_confs():
        """
        Compare current max timestamps vs orchestrator watermarks and
        trigger only the KPI DAGs whose base tables have advanced.
        """
        try:
            hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)

            default_epoch = "1970-01-01 00:00:00"
            watermarks = {
                "watermark_employee_daily_attendance": default_epoch,
                "watermark_da_productivity": default_epoch,
                "watermark_shopper_productivity": default_epoch,
                "watermark_vehicle_productivity": default_epoch,
                "watermark_fc_productivity": default_epoch,
                "watermark_resource_productivity_pc": default_epoch,
                "watermark_resource_level_productivity": default_epoch,
            }

            wm_sql = f"""
            SELECT
                dag_id,
                watermark_employee_daily_attendance,
                watermark_da_productivity,
                watermark_shopper_productivity,
                watermark_vehicle_productivity,
                watermark_fc_productivity,
                watermark_resource_productivity_pc,
                watermark_resource_level_productivity,
                updated_ts
            FROM audit.{watermark_table}
            WHERE dag_id = '{dag_id}'
            ORDER BY updated_ts DESC
            LIMIT 1
            """
            wm_rows = hook.execute(wm_sql)
            if wm_rows:
                (
                    _dag_id,
                    wm_emp,
                    wm_da,
                    wm_shopper,
                    wm_vehicle,
                    wm_fc,
                    wm_res_pc,
                    wm_res_lvl,
                    _updated_ts,
                ) = wm_rows[0]
                mapping = {
                    "watermark_employee_daily_attendance": wm_emp,
                    "watermark_da_productivity": wm_da,
                    "watermark_shopper_productivity": wm_shopper,
                    "watermark_vehicle_productivity": wm_vehicle,
                    "watermark_fc_productivity": wm_fc,
                    "watermark_resource_productivity_pc": wm_res_pc,
                    "watermark_resource_level_productivity": wm_res_lvl,
                }
                for key, value in mapping.items():
                    if value is not None:
                        watermarks[key] = value.strftime("%Y-%m-%d %H:%M:%S")

            # Compute current max timestamps per table
            def _max_ts(sql: str):
                rows = hook.execute(sql)
                if not rows:
                    return None
                (ts,) = rows[0]
                if ts is None:
                    return None
                return ts.strftime("%Y-%m-%d %H:%M:%S")

            current_max = {
                "watermark_employee_daily_attendance": _max_ts(
                    f"SELECT max(toDateTime(created_ts)) AS ts FROM {silver_db}.{source_table_employee_attendance}"
                ),
                "watermark_da_productivity": _max_ts(
                    f"SELECT max(toDateTime(created_ts)) AS ts FROM {silver_db}.{source_table_da_productivity}"
                ),
                "watermark_shopper_productivity": _max_ts(
                    f"SELECT max(toDateTime(created_ts)) AS ts FROM {silver_db}.{source_table_shopper_productivity}"
                ),
                "watermark_vehicle_productivity": _max_ts(
                    f"SELECT max(toDateTime(created_ts)) AS ts FROM {silver_db}.{source_table_vehicle_productivity}"
                ),
                "watermark_fc_productivity": _max_ts(
                    f"SELECT max(toDateTime(created_ts)) AS ts FROM {silver_db}.{source_table_fc_productivity}"
                ),
                "watermark_resource_productivity_pc": _max_ts(
                    f"SELECT max(toDateTime(created_ts)) AS ts FROM {silver_db}.{source_table_resource_productivity_pc}"
                ),
                "watermark_resource_level_productivity": _max_ts(
                    f"SELECT max(toDateTime(event_ts)) AS ts FROM {silver_db}.{source_table_resource_level_productivity}"
                ),
            }

            changed_tables = set()
            for key, cur_ts in current_max.items():
                if cur_ts is None:
                    continue
                if cur_ts > watermarks[key]:
                    changed_tables.add(key)

            logging.info(f"Changed watermark keys: {changed_tables}")

            if not changed_tables:
                logging.info("No base tables advanced beyond orchestrator watermark. Nothing to trigger.")
                # Still update orchestrator watermark row to reflect current_max (even if None)
                values = {}
                for key, ts_str in current_max.items():
                    if ts_str is None:
                        values[key] = "toDateTime('1970-01-01 00:00:00')"
                    else:
                        values[key] = f"toDateTime('{ts_str}')"

                insert_sql = f"""
                INSERT INTO audit.{watermark_table} (
                    dag_id,
                    watermark_employee_daily_attendance,
                    watermark_da_productivity,
                    watermark_shopper_productivity,
                    watermark_vehicle_productivity,
                    watermark_fc_productivity,
                    watermark_resource_productivity_pc,
                    watermark_resource_level_productivity,
                    updated_ts
                )
                VALUES (
                    '{dag_id}',
                    {values['watermark_employee_daily_attendance']},
                    {values['watermark_da_productivity']},
                    {values['watermark_shopper_productivity']},
                    {values['watermark_vehicle_productivity']},
                    {values['watermark_fc_productivity']},
                    {values['watermark_resource_productivity_pc']},
                    {values['watermark_resource_level_productivity']},
                    now()
                )
                """
                hook.execute(insert_sql)
                return {}

            # Map watermark keys to DAGs
            dags_to_trigger: Dict[str, Dict[str, Any]] = {}

            # gz_input_dashboard_kpi_dag depends on all 7
            if changed_tables:
                dags_to_trigger["gz_input_dashboard_kpi_dag"] = {}

            # gz_output_dashboard_kpi_dag depends on fc_productivity, vehicle_productivity, resource_productivity
            if changed_tables & {
                "watermark_fc_productivity",
                "watermark_vehicle_productivity",
                "watermark_resource_productivity_pc",
            }:
                dags_to_trigger["gz_output_dashboard_kpi_dag"] = {}

            # gz_resource_trends_kpi_dag depends on attendance, all productivity, resource_productivity
            if changed_tables & {
                "watermark_employee_daily_attendance",
                "watermark_da_productivity",
                "watermark_shopper_productivity",
                "watermark_vehicle_productivity",
                "watermark_fc_productivity",
                "watermark_resource_productivity_pc",
            }:
                dags_to_trigger["gz_resource_trends_kpi_dag"] = {}

            # gz_fc_operational_trends_kpi_dag depends on fc_productivity, resource_productivity
            if changed_tables & {
                "watermark_fc_productivity",
                "watermark_resource_productivity_pc",
            }:
                dags_to_trigger["gz_fc_operational_trends_kpi_dag"] = {}

            if not dags_to_trigger:
                logging.info("No KPI DAGs need triggering for this delta.")
                return {}

            logging.info(f"Will trigger KPI DAGs (if any conf present): {list(dags_to_trigger.keys())}")

            # Build per-DAG conf payloads that TriggerDagRunOperator will use
            per_dag_conf: Dict[str, Any] = {}
            now = timezone.utcnow()
            for kpi_dag_id in dags_to_trigger.keys():
                # Determine which watermark keys are relevant to this DAG
                if kpi_dag_id == "gz_input_dashboard_kpi_dag":
                    relevant_keys = set(current_max.keys())
                elif kpi_dag_id == "gz_output_dashboard_kpi_dag":
                    relevant_keys = {
                        "watermark_fc_productivity",
                        "watermark_vehicle_productivity",
                        "watermark_resource_productivity_pc",
                    }
                elif kpi_dag_id == "gz_resource_trends_kpi_dag":
                    relevant_keys = {
                        "watermark_employee_daily_attendance",
                        "watermark_da_productivity",
                        "watermark_shopper_productivity",
                        "watermark_vehicle_productivity",
                        "watermark_fc_productivity",
                        "watermark_resource_productivity_pc",
                    }
                elif kpi_dag_id == "gz_fc_operational_trends_kpi_dag":
                    relevant_keys = {
                        "watermark_fc_productivity",
                        "watermark_resource_productivity_pc",
                    }
                else:
                    relevant_keys = set()

                # Only consider keys that actually changed
                relevant_changed = relevant_keys & changed_tables
                if not relevant_changed:
                    continue

                per_dag_watermarks = {k: watermarks[k] for k in relevant_changed}
                per_dag_current_max = {k: current_max[k] for k in relevant_changed}

                per_dag_conf[kpi_dag_id] = {
                    "triggered_by": dag_id,
                    "delta_info": {
                        "watermarks": per_dag_watermarks,
                        "current_max": per_dag_current_max,
                    },
                    "orchestrator_run_id": f"delta__{now.isoformat()}",
                }

            # Update orchestrator watermarks to current_max (single source of truth)
            values = {}
            for key, ts_str in current_max.items():
                if ts_str is None:
                    values[key] = "toDateTime('1970-01-01 00:00:00')"
                else:
                    values[key] = f"toDateTime('{ts_str}')"

            insert_sql = f"""
            INSERT INTO audit.{watermark_table} (
                dag_id,
                watermark_employee_daily_attendance,
                watermark_da_productivity,
                watermark_shopper_productivity,
                watermark_vehicle_productivity,
                watermark_fc_productivity,
                watermark_resource_productivity_pc,
                watermark_resource_level_productivity,
                updated_ts
            )
            VALUES (
                '{dag_id}',
                {values['watermark_employee_daily_attendance']},
                {values['watermark_da_productivity']},
                {values['watermark_shopper_productivity']},
                {values['watermark_vehicle_productivity']},
                {values['watermark_fc_productivity']},
                {values['watermark_resource_productivity_pc']},
                {values['watermark_resource_level_productivity']},
                now()
            )
            """
            hook.execute(insert_sql)

            return {k: v for k, v in per_dag_conf.items() if v is not None}
        except Exception as ex:
            push_failure(error=ex)
            raise

    decide = decide_and_prepare_confs()

    # Extract per-DAG confs from the mapping
    @task()
    def get_conf_for_input(all_confs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return all_confs.get("gz_input_dashboard_kpi_dag")
        except Exception as ex:
            push_failure(error=ex)
            raise

    @task()
    def get_conf_for_output(all_confs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return all_confs.get("gz_output_dashboard_kpi_dag")
        except Exception as ex:
            push_failure(error=ex)
            raise

    @task()
    def get_conf_for_resource_trends(all_confs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return all_confs.get("gz_resource_trends_kpi_dag")
        except Exception as ex:
            push_failure(error=ex)
            raise

    @task()
    def get_conf_for_fc_operational(all_confs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return all_confs.get("gz_fc_operational_trends_kpi_dag")
        except Exception as ex:
            push_failure(error=ex)
            raise

    input_conf = get_conf_for_input(decide)
    output_conf = get_conf_for_output(decide)
    resource_trends_conf = get_conf_for_resource_trends(decide)
    fc_operational_conf = get_conf_for_fc_operational(decide)

    @task.short_circuit
    def check_should_trigger(conf: Optional[Dict[str, Any]]) -> bool:
        """Return True when conf is not None (DAG is affected by changed tables)."""
        try:
            return conf is not None
        except Exception as ex:
            push_failure(error=ex)
            raise

    # Short-circuit: only run trigger when the DAG is in per_dag_conf (conf is not None)
    check_trigger_input = check_should_trigger.override(task_id="check_trigger_input")(input_conf)
    check_trigger_output = check_should_trigger.override(task_id="check_trigger_output")(output_conf)
    check_trigger_resource_trends = check_should_trigger.override(
        task_id="check_trigger_resource_trends"
    )(resource_trends_conf)
    check_trigger_fc_operational = check_should_trigger.override(
        task_id="check_trigger_fc_operational"
    )(fc_operational_conf)

    # Trigger child KPI DAGs only when their source tables have changed
    trigger_input = TriggerDagRunOperator(
        task_id="trigger_gz_input_dashboard_kpi_dag",
        trigger_dag_id="gz_input_dashboard_kpi_dag",
        conf=input_conf,
        wait_for_completion=False,
    )

    trigger_output = TriggerDagRunOperator(
        task_id="trigger_gz_output_dashboard_kpi_dag",
        trigger_dag_id="gz_output_dashboard_kpi_dag",
        conf=output_conf,
        wait_for_completion=False,
    )

    trigger_resource_trends = TriggerDagRunOperator(
        task_id="trigger_gz_resource_trends_kpi_dag",
        trigger_dag_id="gz_resource_trends_kpi_dag",
        conf=resource_trends_conf,
        wait_for_completion=False,
    )

    trigger_fc_operational = TriggerDagRunOperator(
        task_id="trigger_gz_fc_operational_trends_kpi_dag",
        trigger_dag_id="gz_fc_operational_trends_kpi_dag",
        conf=fc_operational_conf,
        wait_for_completion=False,
    )

    wait_for_any_delta >> decide
    decide >> [input_conf, output_conf, resource_trends_conf, fc_operational_conf]

    # Only trigger DAGs that are affected (conf is not None)
    input_conf >> check_trigger_input >> trigger_input
    input_conf >> trigger_input
    output_conf >> check_trigger_output >> trigger_output
    output_conf >> trigger_output
    resource_trends_conf >> check_trigger_resource_trends >> trigger_resource_trends
    resource_trends_conf >> trigger_resource_trends
    fc_operational_conf >> check_trigger_fc_operational >> trigger_fc_operational
    fc_operational_conf >> trigger_fc_operational

    # ------------------------------------------------------------------
    # Email alerting for all tasks
    # ------------------------------------------------------------------
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

        [
            wait_for_any_delta, 
            decide, input_conf, 
            output_conf, 
            resource_trends_conf, 
            fc_operational_conf, 
            check_trigger_input, 
            check_trigger_output, 
            check_trigger_resource_trends, 
            check_trigger_fc_operational, 
            trigger_input, 
            trigger_output, 
            trigger_resource_trends, 
            trigger_fc_operational
        ] >> failure_alert


gz_kpi_delta_orchestrator_dag = gz_kpi_delta_orchestrator_dag()

