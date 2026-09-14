import dagster as dg


@dg.asset_check(asset="ingest_sql")
def check_gold_asset(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    return dg.AssetCheckResult(
        passed=False,
        metadata={"reason": dg.MetadataValue.text("Not implemented yet")},
    )
