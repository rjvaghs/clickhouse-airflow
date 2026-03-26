import yaml
from pathlib import Path
import os

class ConfigLoader:
    """
    Unified configuration loader for environment and table configurations.
    Provides structured dictionary-based output that merges relevant data.
    """

    def __init__(
        self,
        env_config_path: str = str(Path(__file__).resolve().parents[1]) + "/config/" + os.getenv("ENVIRONMENT") + ".yaml",
        table_config_path: str = str(Path(__file__).resolve().parents[1]) + "/config/table_config.yaml",
    ):
        self.env_config_path = Path(env_config_path)
        self.table_config_path = Path(table_config_path)
        self.env_config = self._load_yaml(self.env_config_path)
        self.table_config = self._load_yaml(self.table_config_path)
        self.env = os.getenv("ENVIRONMENT")

    def _load_yaml(self, path: Path):
        """Load and return YAML data."""
        if not path.exists():
            raise FileNotFoundError(f"Config file not found at: {path}")
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def load_env_config(self) -> dict:
        """
        Return full environment configuration as a dict.
        Example: loader.load_env_config("dev")
        """
        environments = self.env_config.get("environments", {})
        env_cfg = environments.get(self.env)
        if not env_cfg:
            raise KeyError(f"Environment '{self.env}' not found in env_config.yaml")
        return env_cfg

    def load_table_config(self, database: str) -> dict:
        """
        Return table and views mapping for the given database.
        Example: loader.load_table_config("bronze")
        """
        databases = self.table_config.get("databases", {})
        layer_cfg = databases.get(database)
        if not layer_cfg:
            raise KeyError(f"Database '{database}' not found in table_config.yaml")
        return layer_cfg

    def load_kafka_connect_config(self) -> dict:
        """
        Return Kafka Connect configuration for a given environment.
        """
        env_cfg = self.load_env_config()
        kafka_cfg = env_cfg.get("kafka-connect") or env_cfg.get("kafka-connect")  # Handle typo fallback
        if not kafka_cfg:
            raise KeyError(f"No 'kafka-connect' section found in environment '{self.env}'")
        return kafka_cfg
