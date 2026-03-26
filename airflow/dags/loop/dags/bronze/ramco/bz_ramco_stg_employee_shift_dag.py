from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from jinja2 import Environment, FileSystemLoader
import os
import logging
from pathlib import Path
import fnmatch
import sys
import re
import requests
import shutil

# ENVIRONMENT = os.getenv("ENVIRONMENT")

# --- Setup Paths ---
common_util_path = Path(__file__).resolve().parents[6]
utils_path = Path(__file__).resolve().parents[4]
dag_path = Path(__file__).resolve().parents[2]
sys.path.append(str(common_util_path))
sys.path.append(str(utils_path))
sys.path.append(str(dag_path))

from loop.utils.load_config import ConfigLoader
from utils.exception_handler import push_failure
from utils.alerting import failure_alert_task

# ------------------------------------------------------------------
# DAG Config
# ------------------------------------------------------------------
ENV = os.getenv("ENVIRONMENT")

ALERT_EMAILS = Variable.get("loop_email_group", deserialize_json=True)

default_args = {
    "owner": "airflow-loop",
    "depends_on_past": False,
    "retries": 1,
    "email": ALERT_EMAILS,
}

dag_id = "bz_ramco_stg_employee_shift_dag"


@dag(
    dag_id=dag_id,
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["clickhouse", "bronze", "ramco"],
)
def stg_to_bz_ramco_employee_shift_ingestion():
    """DAG to load employee shift data from NFS to bronze and archive input file."""
    cfg_loader = ConfigLoader()
    env_cfg = cfg_loader.load_env_config()
    env = os.getenv("ENVIRONMENT")
    clickhouse_conn_id = env_cfg["clickhouse"]["conn_id"]

    # --- Task 1: List files ---
    @task()
    def list_files():
        try:
            nfs_cfg = env_cfg["nfs"]

            base_url = nfs_cfg["base_url"]
            process_path = nfs_cfg["directories"]["ramco-shift"]["process_directory"]
            file_prefix = nfs_cfg["directories"]["ramco-shift"]["file_prefix"]

            directory_url = f"{base_url}{process_path}/"

            logging.info(f"Fetching file list from {directory_url}")

            resp = requests.get(directory_url, timeout=30)
            resp.raise_for_status()

            files_metadata = resp.json()

            matching_files = [
                f["name"]
                for f in files_metadata
                if f.get("type") == "file"
                and fnmatch.fnmatch(f.get("name").lower(), file_prefix)
            ]

            if not matching_files:
                raise ValueError("No matching files present in the server.")

            logging.info(f"Matching files found: {matching_files}")

            return matching_files

        except Exception as ex:
            push_failure(error=ex)
            raise

    # --- Task 2: Create Batch ID from file names ---
    @task()
    def create_batch_id(files: list):
        try:
            batch_ids = []
            for file in files:
                batch_id = re.sub(r'[^A-Za-z0-9]', '', re.sub(r'\.csv$', '', file))
                batch_ids.append(batch_id)
            logging.info(f"Created batch IDs: {batch_ids}")
            return batch_ids
        except Exception as ex:
            push_failure(error=ex)
            raise

    # --- Task 3: Delete existing batch IDs in bronze table ---
    @task()
    def delete_existing_batch(batch_ids: list):
        try:
            if not batch_ids:
                logging.info("No batch IDs to delete. Skipping delete step.")
                return

            # env_cfg = cfg_loader.load_env_config(ENVIRONMENT)
            clickhouse_conn_id = env_cfg["clickhouse"]["conn_id"]

            bronze_db = "bronze"
            table_key = "employee_shift"
            table_cfg = cfg_loader.load_table_config(bronze_db)
            if ENV != "dev":
                bronze_table = table_cfg["local_tables"][table_key]
            else:
                bronze_table = table_cfg["tables"][table_key]
            cluster = env_cfg["clickhouse"]["cluster"]

            dag_dir = Path(__file__).resolve().parents[2]
            SQL_TEMPLATE_DIR = os.path.join(dag_dir, "sql_commons", "dml")
            jinja_env = Environment(loader=FileSystemLoader(SQL_TEMPLATE_DIR))
            template = jinja_env.get_template("delete_table_batch.sql.jinja")

            delete_sql = template.render(
                db=bronze_db, table=bronze_table, batch_ids=batch_ids, cluster=cluster, env=env
            )

            hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)
            logging.info(f"Executing delete SQL for {batch_ids}")
            try:
                hook.execute(delete_sql)
                logging.info(f"Executed delete SQL for {batch_ids}.")
            except Exception as ex:
                logging.error(f"Failed delete SQL for {batch_ids}: {ex}")
                raise
        except Exception as ex:
            push_failure(error=ex)
            raise

    # --- Task 4: Insert new batch data ---
    @task()
    def insert_batch_data(batch_ids: list, files: list):
        try:
            # env_cfg = cfg_loader.load_env_config(ENVIRONMENT)
            print("------------", files)
            clickhouse_conn_id = env_cfg["clickhouse"]["conn_id"]
            nfs_cfg = env_cfg["nfs"]

            base_url = nfs_cfg["base_url"]
            process_path = nfs_cfg["directories"]["ramco-shift"]["process_directory"]
            file_prefix = nfs_cfg["directories"]["ramco-shift"]["file_prefix"]

            directory_url = f"{base_url}{process_path}/"

            bronze_db = "bronze"
            table_key = "employee_shift"
            table_cfg = cfg_loader.load_table_config(bronze_db)
            bronze_table = table_cfg["tables"][table_key]

            current_dir = Path(__file__).resolve().parent
            SQL_TEMPLATE_DIR = os.path.join(current_dir, "sql")
            jinja_env = Environment(loader=FileSystemLoader(SQL_TEMPLATE_DIR))
            template = jinja_env.get_template("bz_ramco_stg_employee_shift.sql.jinja")

            for i in files:
                insert_sql = template.render(
                    bronze_db=bronze_db,
                    bronze_table=bronze_table,
                    dag_id=dag_id,
                    file_prefix=file_prefix,
                    process_path=process_path,
                    base_url=base_url,
                    file=i
                )

                hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)
                logging.info(f"Inserting batch data for {i} into {bronze_table}")
                try:
                    hook.execute(insert_sql)
                    logging.info(f"Inserted data for batches: {i}")
                except Exception as ex:
                    logging.error(f"Error inserting batch data for {i}: {ex}")
                    raise
        except Exception as ex:
            push_failure(error=ex)
            raise

    # --- Task 5: Archive processed files ---
    @task()
    def archive_file(files: list):
        try:
            if not files:
                logging.info("No files to archive. Skipping archive step.")
                return

            nfs_cfg = env_cfg["nfs"]
            process_dir = nfs_cfg["directories"]["ramco-shift"]["process_directory"].rstrip("/")
            archive_dir = nfs_cfg["directories"]["ramco-shift"]["archive_directory"].rstrip("/")
            nfs_conn_id = nfs_cfg["conn_id"]
            moved_files = []

            nfs_hook = SFTPHook(ssh_conn_id=nfs_conn_id)

            with nfs_hook.get_conn() as sftp:
                # Ensure archive directory exists
                try:
                    sftp.listdir(archive_dir)
                except IOError:
                    logging.info(f"Archive directory not found. Creating: {archive_dir}")
                    sftp.mkdir(archive_dir)

                for file in files:
                    src_path = f"{process_dir}/{file}"
                    dest_path = f"{archive_dir}/{file}"

                    logging.info(f"Moving file: {src_path} → {dest_path}")

                    try:
                        # Preferred way: atomic rename (fast, safe)
                        sftp.rename(src_path, dest_path)

                        moved_files.append(file)

                    except IOError:
                        # Fallback if rename fails (cross-filesystem case)
                        logging.warning(
                            f"Rename failed, falling back to copy+delete for file: {file}"
                        )

                        with sftp.open(src_path, "rb") as src, sftp.open(dest_path, "wb") as dst:
                            shutil.copyfileobj(src, dst)

                        sftp.remove(src_path)
                        moved_files.append(file)  
        except Exception as ex:
            push_failure(error=ex)
            raise 

    @task()
    def prepare_silver_conf(files: list):
        """
        Extract event_date range from filenames and pass to Silver
        """

        try:
            dates = []

            nfs_cfg = env_cfg["nfs"]
            file_prefix = nfs_cfg["directories"]["ramco-shift"]["file_prefix"]

            def extract_date(file_name: str):
                match = re.search(r"(\d{1,2}-\d{1,2}-\d{4})", file_name)
                if not match:
                    raise ValueError("No date found in filename")
                return datetime.strptime(match.group(1), "%d-%m-%Y")

            for file in files:
                # expects YYYY-MM-DD in filename
                event_dt = extract_date(file)
                dates.append(event_dt)

            if not dates:
                raise ValueError("No event_date found in filenames")

            return {
                "event_date_from": min(dates).date().isoformat(),
                "event_date_to": max(dates).date().isoformat(),
            }
            
        except Exception as ex:
            push_failure(error=ex)
            raise

    # --- DAG Flow ---
    list_files_task = list_files()
    create_batch_id_task = create_batch_id(list_files_task)
    delete_existing_batch_task = delete_existing_batch(create_batch_id_task)
    insert_batch_data_task = insert_batch_data(create_batch_id_task, list_files_task)
    archive_file_task = archive_file(list_files_task)
    trigger_silver_dag_task = prepare_silver_conf(list_files_task) 

    # --- Trigger Silver DAG ---
    trigger_silver_dag = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="sz_ramco_bz_employee_shift_dag",
        conf=trigger_silver_dag_task,
        wait_for_completion=False,
    )

    list_files_task >> create_batch_id_task >> delete_existing_batch_task \
        >> insert_batch_data_task >> archive_file_task \
        >> trigger_silver_dag_task >> trigger_silver_dag

    # --- ENV-Based Alert Toggle (ADDED ONLY) ---
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

        list_files_task >> create_batch_id_task >> delete_existing_batch_task >> insert_batch_data_task >> archive_file_task >> trigger_silver_dag_task >> trigger_silver_dag

        [
            list_files_task,
            create_batch_id_task,
            delete_existing_batch_task,
            insert_batch_data_task,
            archive_file_task,
            trigger_silver_dag_task,
            trigger_silver_dag,
        ] >> failure_alert
    else:
        list_files_task >> create_batch_id_task >> delete_existing_batch_task >> insert_batch_data_task >> archive_file_task >> trigger_silver_dag_task >> trigger_silver_dag

# --- DAG Instance ---
stg_to_bz_ramco_employee_shift_ingestion = stg_to_bz_ramco_employee_shift_ingestion()
