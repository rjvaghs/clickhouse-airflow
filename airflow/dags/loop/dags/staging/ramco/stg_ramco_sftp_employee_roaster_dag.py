from fnmatch import fnmatch
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.timetables.trigger import CronTriggerTimetable
import pendulum
from airflow.providers.sftp.hooks.sftp import SFTPHook
from datetime import datetime, timedelta
from pathlib import Path
import logging
import shutil
import sys
import os

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

dag_id = "stg_ramco_sftp_employee_roaster_dag"

default_args = {
    "owner": "airflow-loop",
    "depends_on_past": False,
    "retries": 2,
    "email": ALERT_EMAILS,
}

# Instantiate Pendulum and set your timezone
local_tz = pendulum.timezone("Asia/Kolkata")

@dag(
    dag_id=dag_id,
    start_date=datetime(2025, 1, 1),
    schedule=CronTriggerTimetable("0 7 * * *", timezone=local_tz),   # 2 AM IST daily
    catchup=False,
    default_args=default_args,
    tags=["sftp", "nfs", "ramco", "bronze"],
)
def sftp_ramco_employee_roaster_ingestion():

    cfg_loader = ConfigLoader()
    env_cfg = cfg_loader.load_env_config()
    clickhouse_conn_id = env_cfg["clickhouse"]["conn_id"]

    @task
    def list_sftp_files():
        """
        List matching files from SFTP based on environment config.
        """

        try:
            sftp_cfg = env_cfg["sftp-ramco"]
            conn_id = sftp_cfg["conn_id"]
            remote_dir = sftp_cfg["directory"]["roaster"]
            file_prefix = env_cfg["nfs"]["directories"]["ramco-roaster"]["file_prefix"]

            sftp_hook = SFTPHook(ftp_conn_id=conn_id)

            logging.info(f"Listing files from SFTP dir: {remote_dir}")
            files = sftp_hook.list_directory(remote_dir)

            matched_files = [f for f in files if fnmatch(f.lower(), file_prefix)]

            if not matched_files:
                raise ValueError("No matching files present in the server.")

            logging.info(f"Matched files: {matched_files}")

            return matched_files
        except Exception as ex:
            push_failure(error=ex)
            raise

    @task
    def copy_to_nfs(files: list):
        """
        Copy files from SFTP to NFS.
        """

        try:
            sftp_cfg = env_cfg["sftp-ramco"]
            sftp_conn_id = sftp_cfg["conn_id"]
            remote_dir = sftp_cfg["directory"]["roaster"]

            nfs_cfg = env_cfg["nfs"]["directories"]["ramco-roaster"]
            nfs_conn_id = env_cfg["nfs"]["conn_id"]
            process_dir = nfs_cfg["process_directory"]

            try:
                for file_name in files:
                    remote_path = f"{remote_dir.rstrip('/')}/{file_name}"
                    local_path = f"{process_dir}/{file_name}"

                    logging.info(f"Copying {remote_path} → {local_path}")

                    try:
                        sftp_hook = SFTPHook(ftp_conn_id=sftp_conn_id)
                        nfs_hook = SFTPHook(ftp_conn_id=nfs_conn_id)
                        with sftp_hook.get_conn() as sftp:
                            with nfs_hook.get_conn() as nfs:
                                with sftp.open(remote_path, "rb") as src, nfs.open(local_path, "wb") as dst:
                                    shutil.copyfileobj(src, dst)
                    except Exception as e:
                        logging.error(f"Error copying {remote_path} → {local_path}: {e}")
                        raise
            except Exception as e:
                logging.error(f"Error copying files: {e}")
                raise

            return files

        except Exception as ex:
            push_failure(error=ex)
            raise

    sftp_files = list_sftp_files()
    copied_files = copy_to_nfs(sftp_files)

    trigger_bronze_dag = TriggerDagRunOperator(
        task_id="trigger_bronze_dag",
        trigger_dag_id="bz_ramco_stg_employee_roaster_dag",
        wait_for_completion=False,
    )

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

        sftp_files >> copied_files >> trigger_bronze_dag 

        [sftp_files, copied_files, trigger_bronze_dag] >> failure_alert

    else:
        sftp_files >> copied_files >> trigger_bronze_dag


sftp_ramco_employee_roaster_ingestion_dag = sftp_ramco_employee_roaster_ingestion()
