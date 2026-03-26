from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context
from pathlib import Path
import sys
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

utils_path = Path(__file__).resolve().parents[1]
sys.path.append(str(utils_path))

from utils.exception_handler import push_failure

class ClickHouseWatermarkTrigger(BaseTrigger):
    """
    Simple trigger that defers re-checking watermarks after a delay.
    The actual watermark comparison is done in the sensor's execute/resume methods.
    """

    def __init__(self, poke_interval: int = 60) -> None:
        super().__init__()
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, Dict[str, Any]]:
        print(self.__class__.__module__)
        print(self.__class__.__name__)
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {"poke_interval": self.poke_interval},
        )

    async def run(self) -> None:  # type: ignore[override]
        import asyncio

        await asyncio.sleep(self.poke_interval)
        yield TriggerEvent({"status": "poke"})


class ClickHouseWatermarkSensor(BaseSensorOperator):
    """
    Deferrable sensor that checks for new data in one or more ClickHouse tables
    by comparing max(timestamp_column) against stored watermarks in an audit table.
    """

    def __init__(
        self,
        *,
        clickhouse_conn_id: str,
        watermark_db: str,
        watermark_table: str,
        dag_id: str,
        sources: Sequence[Dict[str, str]],
        poke_interval: int = 60,
        deferrable: bool = True,
        **kwargs: Any,
    ) -> None:
        # Remove deferrable from kwargs to avoid passing it twice to BaseSensorOperator
        kwargs.pop("deferrable", None)
        super().__init__(**kwargs)
        self.deferrable = deferrable
        self.clickhouse_conn_id = clickhouse_conn_id
        self.watermark_db = watermark_db
        self.watermark_table = watermark_table
        self.dag_id_for_watermark = dag_id
        self.sources: List[Dict[str, str]] = list(sources)
        self.poke_interval = poke_interval

    def _get_hook(self) -> ClickHouseHook:
        return ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)

    def _fetch_current_watermarks(self, hook: ClickHouseHook) -> Dict[str, Optional[str]]:
        """
        Returns current watermarks as ISO strings per source key, or None if not set.
        """
        sql = f"""
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
        FROM {self.watermark_db}.{self.watermark_table}
        WHERE dag_id = '{self.dag_id_for_watermark}'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
        rows = hook.execute(sql)
        if not rows:
            return {}

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
        ) = rows[0]

        watermarks: Dict[str, Optional[str]] = {}
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
            watermarks[key] = value.isoformat() if value is not None else None
        return watermarks

    def _has_new_data(self, hook: ClickHouseHook) -> bool:
        """
        Check if any source table has max(timestamp_column) greater than stored watermark.
        """
        current_watermarks = self._fetch_current_watermarks(hook)
        for src in self.sources:
            table_fqn = f"{src['database']}.{src['table']}"
            ts_column = src["timestamp_column"]
            watermark_column_name = src["watermark_column_name"]

            sql = f"SELECT max(toDateTime({ts_column})) AS max_ts FROM {table_fqn}" 
            rows = hook.execute(sql)
            if not rows:
                continue

            (max_ts,) = rows[0]
            if max_ts is None:
                continue

            stored_iso = current_watermarks.get(watermark_column_name)
            if stored_iso is None:
                # No watermark yet for this source => treat as new data
                return True

            # Compare as strings; ClickHouse returns pandas.Timestamp which has isoformat
            if max_ts.isoformat() > stored_iso:
                return True

        return False

    def execute(self, context: Context) -> Any:
        try:
            hook = self._get_hook()
            if self._has_new_data(hook):
                return True

            self.defer(
                trigger=ClickHouseWatermarkTrigger(poke_interval=self.poke_interval),
                method_name="execute_complete",
            )
        except Exception as ex:
            push_failure(error=ex)
            raise

    def execute_complete(
        self,
        context: Context,
        event: Optional[Dict[str, Any]] = None,
    ) -> Any:
        try:
            hook = self._get_hook()
            if self._has_new_data(hook):
                return True

            # No new data yet; defer again
            self.defer(
                trigger=ClickHouseWatermarkTrigger(poke_interval=self.poke_interval),
                method_name="execute_complete",
            )
        except Exception as ex:
            push_failure(error=ex)
            raise

