import dagster as dg


@dg.schedule(cron_schedule="0 0 * * *", target="*")  # type: ignore[reportUnknownVariableType]
def daily_mlflow_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest | dg.SkipReason:
    return dg.RunRequest()
