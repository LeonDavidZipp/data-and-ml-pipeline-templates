import dagster as dg


@dg.schedule(cron_schedule="0 0 * * *", target="*")  # type: ignore[reportUnknownVariableType]
def daily_ingest_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest | dg.SkipReason:
    return dg.RunRequest()


INTERVAL_MINUTES = 60 * 5


@dg.sensor(target="*", minimum_interval_seconds=INTERVAL_MINUTES)  # type: ignore[reportUnknownVariableType]
def bronze_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.RunRequest | dg.SkipReason:
    return dg.RunRequest()
