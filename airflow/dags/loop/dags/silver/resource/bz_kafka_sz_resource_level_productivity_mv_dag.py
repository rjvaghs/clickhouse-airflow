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

dag_id = "bz_kafka_sz_resource_level_productivity_mv_dag"

@dag(
    dag_id=dag_id,
    schedule=None,
    catchup=False,
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    tags=["clickhouse", "silver", "mv"],
)
def sz_kafka_bz_resource_level_productivity_mv_aggregation():
    cfg_loader = ConfigLoader()
    env = os.getenv("ENVIRONMENT")

    @task()
    def drop_view():
        """
        Loads environment configs and executes a templated SQL.
        """

        # env=ENVIRONMENT

        clickhouse_conn_id = cfg_loader.load_env_config()["clickhouse"]["conn_id"]

        target_db = "silver"
        table_key = "resource_level_productivity"
        cluster = cfg_loader.load_env_config()["clickhouse"]["cluster"]
        cfg_target = cfg_loader.load_table_config(target_db)
        target_view = cfg_target["views"][table_key]

        sql = jinja_drop_env.get_template("drop_view.sql.jinja").render(
            database=target_db,
            view=target_view,
            cluster=cluster,
            env=env
        )

        hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)

        logging.info(f"Executing rendered SQL for {dag_id}")
        try:
            hook.execute(sql)
            logging.info(f"Successfully dropped view {target_view} in {target_db}")
        except Exception as ex:
            logging.error(f"Failed to drop view {target_view} in {target_db}: {ex}")
            raise

    @task()
    def create_view():
        """
        Loads environment configs and executes a templated SQL.
        """

        clickhouse_conn_id = cfg_loader.load_env_config()["clickhouse"]["conn_id"]

        target_db = "silver"
        source_db = "bronze"
        table_key = "resource_level_productivity"
        cfg_target = cfg_loader.load_table_config(target_db)
        cfg_source = cfg_loader.load_table_config(source_db)

        target_view = cfg_target["views"][table_key]

        target_table = cfg_target["tables"][table_key]

        if env == 'dev':
            source_table = cfg_source["tables"][table_key]
        else:
            source_table = cfg_source["local_tables"][table_key]

        target_cluster = cfg_loader.load_env_config()["clickhouse"]["cluster"]

        hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)

        template = jinja_env.get_template("sz_resource_level_productivity_mv.sql.jinja")

        sql = template.render(
            target_db=target_db,
            target_table=target_table,
            target_view=target_view,
            source_db=source_db,
            source_table=source_table,
            target_cluster=target_cluster,
            env=env
        )

        logging.info(f"Executing rendered SQL for {dag_id}")
        try:
            hook.execute(sql)
            logging.info(f"Successfully created view {target_view} in {target_db}")
        except Exception as ex:
            logging.error(f"Failed to create view {target_view} in {target_db}: {ex}")
            raise

    drop = drop_view()
    create = create_view()
    drop >> create

sz_kafka_bz_resource_level_productivity_mv_aggregation = sz_kafka_bz_resource_level_productivity_mv_aggregation()
