"""Gold layer — business-ready aggregations and feature tables.

Each asset reads silver Delta tables, computes final aggregations or
feature sets, and writes to the gold bucket as Delta.
"""

import dagster as dg
import polars as pl
from deltalake import DeltaTable  # type: ignore
from deltalake.exceptions import TableNotFoundError

from datascience_as_a_service.defs.resources import S3Resource


@dg.asset(group_name="gold", deps=["changeme"])
def gold_summary(
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

    arrow_data = df.to_arrow()  # type: ignore
    try:
        (
            DeltaTable(target_uri, storage_options=opts)
            .merge(
                arrow_data,  # type: ignore
                predicate="t.placeholder = s.placeholder",
                target_alias="t",
                source_alias="s",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    except TableNotFoundError:
        df.write_delta(  # type: ignore
            target_uri,
            mode="error",
            storage_options=opts,
        )
    except Exception as e:
        raise e

    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )
