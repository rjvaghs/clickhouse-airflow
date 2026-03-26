from airflow.decorators import dag, task
from datetime import datetime, timedelta
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import json
import os
import logging
import sys
import requests

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
    dag_id="deploy_region_clickhouse_sink_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["kafka-connect", "clickhouse", "deployment"],
)
def deploy_region_clickhouse_sink():
    cfg_loader = ConfigLoader()

    @task()
    def generate_clickhouse_sink_json():
        """
        Dynamically generate clickhouse-sink.json files for all connectors in the given environment.
        """
        
        kafka_connect_cfg = cfg_loader.load_kafka_connect_config()
        endpoint = kafka_connect_cfg.get("endpoint")
        connectors = kafka_connect_cfg.get("connector", {})

        name = "region"
        topic = connectors[name]["topic"]
        table = connectors[name]["table"]

        env = os.getenv("ENVIRONMENT")

        template = f"clickhouse-sink-template-{env}.yaml.jinja"

        current_dir = Path(__file__).resolve().parent
        CLICKHOUSE_SINK_TEMPLATE_DIR = os.path.join(current_dir, "clickhouse-sink-template")
        jinja_env = Environment(loader=FileSystemLoader(CLICKHOUSE_SINK_TEMPLATE_DIR))
        template = jinja_env.get_template(template)

        connector_config = template.render(
            name=name,
            topic=topic,
            table=table
        )

        connector_config = json.loads(connector_config)

        output_path = os.path.join(current_dir, "connector", f"{name}_clickhouse_sink.json")
        with open(output_path, "w") as f:
            json.dump(connector_config, f, indent=4)

        logging.info(f"Generated connector config at: {output_path}")

        return {"name": name, "connector_config": connector_config, "endpoint": endpoint}

    @task()
    def delete_existing_connector(connector_info: dict):
        """
        Delete existing Kafka connector if it exists (using requests).
        """
        name = connector_info["name"]

        connector_name = f"{name}_clickhouse_sink"
        kafka_connect_url = connector_info["endpoint"]

        connector_url = f"{kafka_connect_url}/connectors/{connector_name}"
        logging.info(f"Checking connector: {connector_name}")

        response = requests.get(connector_url)

        if response.status_code == 200:
            logging.info(f"Deleting existing connector: {connector_name}")
            del_response = requests.delete(connector_url)
            if del_response.status_code in [200, 204]:
                logging.info("Connector deleted successfully.")
            else:
                logging.error(f"Failed to delete connector. Status: {del_response.status_code}, "
                              f"Response: {del_response.text}")
                raise RuntimeError("Failed to delete connector.")
        elif response.status_code == 404:
            logging.info("No existing connector found, proceeding to creation.")
        else:
            logging.error(f"Error checking connector. Status: {response.status_code}, "
                          f"Response: {response.text}")
            raise RuntimeError("Failed to check connector state.")

    @task()
    def create_new_connector(connector_info: dict):
        """
        Create new Kafka connector using generated JSON (using requests).
        """

        connector_config = connector_info["connector_config"]
        endpoint = connector_info["endpoint"]
        kafka_connect_url = f"{endpoint}/connectors"

        logging.info(f"Creating connector with payload:\n{json.dumps(connector_config, indent=2)}")

        response = requests.post(
            kafka_connect_url,
            headers={"Content-Type": "application/json"},
            json=connector_config
        )

        if response.status_code in [200, 201]:
            logging.info("Connector created successfully.")
        else:
            logging.error(f"Failed to create connector. Status: {response.status_code}, "
                          f"Response: {response.text}")
            raise RuntimeError("Connector creation failed.")

    # --- DAG FLOW ---
    connector_info = generate_clickhouse_sink_json()
    delete_task = delete_existing_connector(connector_info)
    create_task = create_new_connector(connector_info)

    connector_info >> delete_task >> create_task


deploy_region_clickhouse_sink = deploy_region_clickhouse_sink()

