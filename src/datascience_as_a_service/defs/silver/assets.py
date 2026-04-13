"""Silver layer — transformed, enriched, and joined data.

Each asset reads one or more bronze Delta tables, applies business logic
(joins, filters, derived columns), and writes to the silver bucket as Delta.
"""

import dagster as dg
import polars as pl

from datascience_as_a_service.defs.resources import S3Resource
from datascience_as_a_service.utils import upsert_deltatable


@dg.asset(
    group_name="silver",
    deps=["changeme"],
    automation_condition=dg.AutomationCondition.eager(),  # type: ignore
)
def assets_silver(
    context: dg.AssetExecutionContext,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    source_bucket = "bronze"
    source_table = "changeme"
    target_bucket = "silver"
    target_table = "changeme"

    opts = s3.delta_storage_options
    source_uri = f"s3://{source_bucket}/{source_table}"
    target_uri = f"s3://{target_bucket}/{target_table}"
    quarantine_uri = f"s3://{target_bucket}/quarantine/{target_table}"

    lf = pl.scan_delta(source_uri, storage_options=opts)

    lf_target = (
        lf
        # TODO: apply transformations
    )

    lf_quarantine = lf.join(lf_target, how="anti", on=["changeme"])

    df_target = lf_target.collect()
    df_quarantine = lf_quarantine.collect()

    context.log.info(
        f"Silver: {df_target.height:,} rows after transform → {target_uri}"
    )
    context.log.info(
        f"Silver: {df_quarantine.height:,} rows quarantined → {quarantine_uri}"
    )

    predicate = "t.changeme = s.changeme"
    target_alias = "t"
    source_alias = "s"
    upsert_deltatable(
        target_uri,
        df_target,
        opts,
        predicate=predicate,
        target_alias=target_alias,
        source_alias=source_alias,
    )
    upsert_deltatable(
        quarantine_uri,
        df_quarantine,
        opts,
        predicate=predicate,
        target_alias=target_alias,
        source_alias=source_alias,
    )
    return dg.MaterializeResult(
        value=None,
        metadata={
            "row_count": dg.MetadataValue.int(df_target.height),
            "quarantined_row_count": dg.MetadataValue.int(df_quarantine.height),
        },
    )
