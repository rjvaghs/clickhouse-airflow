from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys
import requests
import os

# ENVIRONMENT = os.getenv("ENVIRONMENT")

# Project path setup
root_path = Path(__file__).resolve().parents[4]
sys.path.append(str(root_path))

from loop.utils.load_config import ConfigLoader

# DAG CONFIG 
default_args = {
    "owner": "airflow-loop",
    "depends_on_past": False,
    "retries": 1,
}

@dag(
    dag_id="pause_fc_productivity_clickhouse_sink_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["kafka-connect", "clickhouse", "pause"],
)
def pause_fc_productivity_clickhouse_sink():

    @task()
    def pause_connector():
        """
        Pause existing Kafka connector if it exists (using requests).
        """

        cfg_loader = ConfigLoader()

        
        kafka_connect_cfg = cfg_loader.load_kafka_connect_config()
        endpoint = kafka_connect_cfg.get("endpoint")
        
        name = "fc_productivity"

        connector_name = f"{name}_clickhouse_sink"

        connector_url = f"{endpoint}/connectors/{connector_name}/pause"
        logging.info(f"Pausing connector: {connector_name}")

        response = requests.put(connector_url)

        if response.status_code in [200, 202]:
            logging.info(f"Connector {connector_name} paused successfully.")
        elif response.status_code == 404:
            logging.warning(f"Connector {connector_name} not found.")
        else:
            logging.error(f"Failed to pause connector. Status: {response.status_code}, "
                          f"Response: {response.text}")
            raise RuntimeError("Failed to pause connector.")

    # DAG FLOW 
    pause_task = pause_connector()

    pause_task


pause_fc_productivity_clickhouse_sink_dag = pause_fc_productivity_clickhouse_sink()

