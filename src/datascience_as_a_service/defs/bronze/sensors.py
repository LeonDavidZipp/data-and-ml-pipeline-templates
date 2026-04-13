import dagster as dg

INTERVAL_MINUTES = 60 * 5


@dg.sensor(target="*", minimum_interval_seconds=INTERVAL_MINUTES)  # type: ignore[reportUnknownVariableType]
def bronze_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.RunRequest | dg.SkipReason:
    return dg.RunRequest()
