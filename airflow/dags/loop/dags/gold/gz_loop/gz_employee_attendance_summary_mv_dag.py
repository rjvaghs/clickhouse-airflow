from airflow.decorators import dag, task
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
import os
from pathlib import Path
import sys
import logging

# ENVIRONMENT = os.getenv("ENVIRONMENT")

# Project paths
root_path = Path(__file__).resolve().parents[4]
current_dir = Path(__file__).resolve().parent
sys.path.append(str(root_path))

from loop.utils.load_config import ConfigLoader

# Default DAG arguments
default_args = {
    "owner": "airflow-loop",
    "depends_on_past": False,
    "retries": 1,
}

# Jinja setup for SQL
SQL_TEMPLATE_DIR = os.path.join(current_dir, "sql")
DROP_SQL_TEMPLATE_DIR = os.path.join(root_path, "dags", "sql_commons", "dml")
jinja_env = Environment(loader=FileSystemLoader(SQL_TEMPLATE_DIR))
jinja_drop_env = Environment(loader=FileSystemLoader(DROP_SQL_TEMPLATE_DIR))

dag_id = "gz_employee_attendance_summary_mv_dag"


@dag(
    dag_id=dag_id,
    schedule=None,
    catchup=False,
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    tags=["clickhouse", "gold", "gz_loop", "mv"],
)
def gz_employee_attendance_summary_mv_aggregation():
    cfg_loader = ConfigLoader()
    env = os.getenv("ENVIRONMENT")

    @task()
    def drop_view():
        """
        Loads environment configs and executes a templated SQL.
        """
        clickhouse_conn_id = cfg_loader.load_env_config()["clickhouse"]["conn_id"]

        database = "gz_loop"
        table_key = "employee_attendance_summary"
        cfg_gold = cfg_loader.load_table_config(database)
        view = cfg_gold["views"][table_key]
        cluster = cfg_loader.load_env_config()["clickhouse"]["cluster"]

        sql = jinja_drop_env.get_template("drop_view.sql.jinja").render(
            database=database,
            view=view,
            cluster=cluster,
            env=env
        )

        hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)

        logging.info(f"Executing rendered SQL for {dag_id}")
        try:
            hook.execute(sql)
            logging.info(f"Successfully dropped view {view} in {database}")
        except Exception as ex:
            logging.error(f"Failed to drop view {view} in {database}: {ex}")
            raise

    @task()
    def create_view():
        """
        Loads environment configs and executes a templated SQL.
        """
        
        clickhouse_conn_id = cfg_loader.load_env_config()["clickhouse"]["conn_id"]

        gold_db = "gz_loop"
        table_key = "employee_attendance_summary"
        cfg_gold = cfg_loader.load_table_config(gold_db)
        gold_view = cfg_gold["views"][table_key]
        gold_table = cfg_gold["tables"][table_key]
        target_cluster = cfg_loader.load_env_config()["clickhouse"]["cluster"]

        silver_db = "silver"
        table_key = "employee_daily_attendance"
        cfg_silver = cfg_loader.load_table_config(silver_db)
        silver_table = cfg_silver["local_tables"][table_key]

        hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)

        template = jinja_env.get_template("mv_employee_attendance_summary_kpi.sql.jinja")

        sql = template.render(
            gold_db=gold_db,
            gold_table=gold_table,
            gold_view=gold_view,
            silver_db=silver_db,
            silver_table=silver_table,
            target_cluster=target_cluster,
            env=env
        )

        logging.info(f"Executing rendered SQL for {dag_id}")
        try:
            hook.execute(sql)
            logging.info(f"Successfully created view {gold_view} in {gold_db}")
        except Exception as ex:
            logging.error(f"Failed to create view {gold_view} in {gold_db}: {ex}")
            raise

    drop = drop_view()
    create = create_view()
    drop >> create

gz_employee_attendance_summary_mv_aggregation = gz_employee_attendance_summary_mv_aggregation()
