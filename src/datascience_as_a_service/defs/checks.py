import dagster as dg


def _get_row_count(
    context: dg.AssetCheckExecutionContext, asset_key: dg.AssetKey
) -> int:
    event = context.instance.get_latest_materialization_event(asset_key)
    if event is None or event.asset_materialization is None:
        return 0
    meta = event.asset_materialization.metadata.get("row_count")
    if meta is None:
        return 0
    return int(meta.value)  # type: ignore[arg-type]


@dg.asset_check(asset="ingest_postgres")
def check_ingest_postgres_not_empty(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    row_count = _get_row_count(context, dg.AssetKey("ingest_postgres"))
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": dg.MetadataValue.int(row_count)},
    )


@dg.asset_check(asset="ingest_duckdb")
def check_ingest_duckdb_not_empty(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    row_count = _get_row_count(context, dg.AssetKey("ingest_duckdb"))
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": dg.MetadataValue.int(row_count)},
    )


@dg.asset_check(asset="ingest_http")
def check_ingest_http_not_empty(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    row_count = _get_row_count(context, dg.AssetKey("ingest_http"))
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": dg.MetadataValue.int(row_count)},
    )


@dg.asset_check(asset="ingest_csv")
def check_ingest_csv_not_empty(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    row_count = _get_row_count(context, dg.AssetKey("ingest_csv"))
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": dg.MetadataValue.int(row_count)},
    )


@dg.asset_check(asset="ingest_parquet")
def check_ingest_parquet_not_empty(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    row_count = _get_row_count(context, dg.AssetKey("ingest_parquet"))
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": dg.MetadataValue.int(row_count)},
    )


@dg.asset_check(asset="ingest_xlsx")
def check_ingest_xlsx_not_empty(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    row_count = _get_row_count(context, dg.AssetKey("ingest_xlsx"))
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": dg.MetadataValue.int(row_count)},
    )


@dg.asset_check(asset="ingest_json")
def check_ingest_json_not_empty(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    row_count = _get_row_count(context, dg.AssetKey("ingest_json"))
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": dg.MetadataValue.int(row_count)},
    )
