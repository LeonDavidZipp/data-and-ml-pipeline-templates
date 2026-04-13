import dagster as dg


@dg.asset_check(asset="assets_mlflow")
def check_model_improvement(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    # TODO: implement comparison of test_rmse across the last two materialisations
    return dg.AssetCheckResult(
        passed=False,
        metadata={"reason": dg.MetadataValue.text("Not implemented yet")},
    )
