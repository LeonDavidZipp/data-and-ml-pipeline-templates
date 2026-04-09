"""Bronze layer — cleaned and validated copies of raw data.

Each asset reads a raw Delta table, applies basic cleaning (drop nulls,
deduplicate, cast types), and writes the result to the bronze bucket as Delta.
"""

import dagster as dg
import polars as pl
from deltalake import DeltaTable  # type: ignore
from deltalake.exceptions import TableNotFoundError

from datascience_as_a_service.defs.resources import S3Resource


@dg.asset(group_name="bronze", deps=["changeme"])
def assets_bronze(
    context: dg.AssetExecutionContext,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    source_bucket = "raw"
    source_table = "changeme"
    target_bucket = "bronze"
    target_table = "changeme"

    opts = s3.delta_storage_options
    source_uri = f"s3://{source_bucket}/{source_table}"
    target_uri = f"s3://{target_bucket}/{target_table}"

    df = (
        pl.scan_delta(source_uri, storage_options=opts)
        # TODO: add transformations here
        .collect()
    )

    context.log.info(f"Bronze: {df.height:,} rows after transform → {target_uri}")

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
