"""Cross-cutting alerting — not tied to any single medallion layer."""

import dagster as dg

from datascience_as_a_service.defs.resources import AlertResource


@dg.run_failure_sensor(monitor_all_code_locations=True)  # type: ignore[reportUnknownVariableType]
def alert_on_run_failure(
    context: dg.RunFailureSensorContext,
    alerts: AlertResource,
) -> None:
    run = context.dagster_run
    message = (
        f"Run failed: job=`{run.job_name}` run_id={run.run_id}\n"
        f"{context.failure_event.message}"
    )
    context.log.error(message)
    alerts.notify(message)
