from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from datetime import datetime
import os
import re
import logging
import sys
import shutil
from pathlib import Path

# ------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------
root_path = Path(__file__).resolve().parents[4]
sys.path.append(str(root_path))

from loop.utils.load_config import ConfigLoader

dag_id = "stg_to_bz_file_movement_dag"

@dag(
    dag_id=dag_id,
    schedule=None,
    catchup=False,
    params={
        "source": None,
        "start_date": None,
        "end_date": None,
    },
    start_date=datetime(2025, 1, 1),
    tags=["reprocessing", "nfs", "bronze"],
)
def move_files_back_to_process():

    cfg_loader = ConfigLoader()
    env_cfg = cfg_loader.load_env_config()

    # ------------------------------------------------------------------
    # Validate params
    # ------------------------------------------------------------------
    @task()
    def validate_and_prepare_params(**context):
        dag_run = context.get("dag_run")
        conf = dag_run.conf or {}
        params = context.get("params") or {}

        source = conf.get("source") or params.get("source")
        start_date = conf.get("start_date") or params.get("start_date")
        end_date = conf.get("end_date") or params.get("end_date")

        if not source:
            raise ValueError("source is mandatory")

        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        except Exception:
            raise ValueError("start_date and end_date must be YYYY-MM-DD")

        if start_date > end_date:
            raise ValueError("start_date cannot be after end_date")

        return {
            "source": source,
            "start_date": start_date,
            "end_date": end_date,
        }

    # ------------------------------------------------------------------
    # Move files from archive ➜ process (NFS via SFTP)
    # ------------------------------------------------------------------
    @task()
    def move_files_back_to_process_task(input_params: dict):
        source = input_params["source"]
        start_date = input_params["start_date"]
        end_date = input_params["end_date"]

        nfs_cfg = env_cfg["nfs"]
        source_cfg = nfs_cfg["directories"][source]

        archive_dir = source_cfg["archive_directory"].rstrip("/")
        process_dir = source_cfg["process_directory"].rstrip("/")
        nfs_conn_id = nfs_cfg["conn_id"]

        logging.info(f"Scanning NFS archive directory: {archive_dir}")

        date_pattern = re.compile(r"(\d{2}-\d{2}-\d{4})")
        moved_files = []

        nfs_hook = SFTPHook(ftp_conn_id=nfs_conn_id)

        with nfs_hook.get_conn() as nfs:
            try:
                files = nfs.listdir(archive_dir)
            except FileNotFoundError:
                logging.warning(f"Archive directory not found: {archive_dir}")
                return

            for file in files:
                match = date_pattern.search(file)
                if not match:
                    continue

                file_date = datetime.strptime(match.group(1), "%d-%m-%Y").date()

                if start_date <= file_date <= end_date:
                    src_key = f"{archive_dir}/{file}"
                    dest_key = f"{process_dir}/{file}"

                    logging.info(f"Reprocessing file: {file}")
                    logging.info(f"Moving {src_key} → {dest_key}")

                    try:
                        with nfs.open(src_key, "rb") as src, nfs.open(dest_key, "wb") as dst:
                            shutil.copyfileobj(src, dst)

                        nfs.remove(src_key)
                        moved_files.append(file)

                    except Exception as ex:
                        logging.error(f"Failed to move file {file}: {ex}")
                        raise

        if not moved_files:
            logging.info("No files matched the given date range")
        else:
            logging.info(f"Moved {len(moved_files)} files for reprocessing")
            logging.info(moved_files)

    # DAG wiring
    input_params = validate_and_prepare_params()
    move_files_back_to_process_task(input_params)

move_files_back_to_process = move_files_back_to_process()
