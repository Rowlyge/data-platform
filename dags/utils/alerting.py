"""
Shared failure-alerting callback for Airflow DAGs in this project.

For a local dev platform, a full email/Slack integration is overkill.
Instead, every task failure (after retries are exhausted) is written as a
structured, appendable log entry to a dedicated alerts file — separate from
the regular task logs — so failures are easy to find and could later be
piped into a real notification channel (email/Slack/PagerDuty) without
changing any DAG code, just this one function.
"""
import json
import os
from datetime import datetime, timezone

ALERTS_LOG_PATH = os.environ.get(
    "AIRFLOW_ALERTS_LOG_PATH", "/opt/airflow/logs/alerts.log"
)


def task_failure_alert(context):
    """on_failure_callback: called once a task instance has exhausted all
    its retries and is being marked as permanently failed."""
    ti = context["task_instance"]
    exception = context.get("exception")

    alert = {
        "alerted_at": datetime.now(timezone.utc).isoformat(),
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context["run_id"],
        "data_interval_start": str(context.get("data_interval_start")),
        "data_interval_end": str(context.get("data_interval_end")),
        "try_number": ti.try_number,
        "max_tries": ti.max_tries,
        "log_url": ti.log_url,
        "exception": str(exception) if exception else None,
    }

    line = json.dumps(alert)

    os.makedirs(os.path.dirname(ALERTS_LOG_PATH), exist_ok=True)
    with open(ALERTS_LOG_PATH, "a") as f:
        f.write(line + "\n")

    # Also print to the task's own log — visible directly in Airflow UI too.
    print(f"[ALERT] Task failed permanently: {line}")
