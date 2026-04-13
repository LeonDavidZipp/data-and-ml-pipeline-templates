"""Gold layer — business-ready aggregations and feature tables.

Each asset reads silver Delta tables, computes final aggregations or
feature sets, and writes to the gold bucket as Delta.
"""

import dagster as dg
import polars as pl

from datascience_as_a_service.defs.resources import S3Resource
from datascience_as_a_service.utils import upsert_deltatable


@dg.asset(
    group_name="gold",
    deps=["changeme"],
    automation_condition=dg.AutomationCondition.eager(),  # type: ignore
)
def assets_gold(
    context: dg.AssetExecutionContext,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    source_bucket = "silver"
    source_table = "changeme"
    target_bucket = "gold"
    target_table = "changeme"

    opts = s3.delta_storage_options
    source_uri = f"s3://{source_bucket}/{source_table}"
    target_uri = f"s3://{target_bucket}/{target_table}"

    df = (
        pl.scan_delta(source_uri, storage_options=opts)
        # TODO: add transformations here
        .collect()
    )

    context.log.info(f"Gold: {df.height:,} rows after transform → {target_uri}")

    upsert_deltatable(
        target_uri,
        df,
        opts,
        predicate="t.id = s.id",
        target_alias="t",
        source_alias="s",
    )
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )
