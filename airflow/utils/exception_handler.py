import traceback
from airflow.operators.python import get_current_context
from typing import Union, Optional


def push_failure(
    *,
    error: Union[Exception, str],
    error_type: Union[str, None] = None,
    include_traceback: bool = True
):
    """
    Push standardized failure_info XCom for the current task.
    Safe for Airflow 3.x.
    """
    ctx = get_current_context()
    ti = ctx["ti"]

    if isinstance(error, Exception):
        message = str(error)
        etype = error_type or type(error).__name__
        tb = traceback.format_exc() if include_traceback else ""
    else:
        message = str(error)
        etype = error_type or "Error"
        tb = ""

    ti.xcom_push(
        key="failure_info",
        value={
            "task_id": ctx["task"].task_id,
            "try_number": ti.try_number,
            "state": "failed",
            "error_type": etype,
            "error_message": message,
            "traceback": tb,
            "log_url": ti.log_url,
        },
    )
