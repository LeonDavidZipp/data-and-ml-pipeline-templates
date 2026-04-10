"""Raw ingestion assets — one asset per source type.

Each asset reads from an external source and will materialise the result as a
Delta table.  Two variables are defined at the top of every asset body:

    target_bucket : str  – S3 bucket where the Delta table will be written
    target_table  : str  – table name used as the S3 key prefix / table path

Additional packages required per asset (install with ``uv add <pkg>``):
    postgres  → psycopg2-binary, polars, connectorx
    duckdb    → duckdb  (already in deps), polars
    http      → httpx, polars
    csv       → polars
    parquet   → polars
    xlsx      → polars, xlsx2csv
    json      → polars
"""

from io import IOBase
from typing import IO, cast

import dagster as dg
import duckdb
import polars as pl
from deltalake import DeltaTable  # type: ignore
from deltalake.exceptions import TableNotFoundError

from datascience_as_a_service.defs.resources import (
    DuckDBResource,
    HttpResource,
    PostgresResource,
    S3Resource,
)

# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_postgres(
    context: dg.AssetExecutionContext,
    postgres: PostgresResource,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    source_key = "changeme"
    target_bucket = "raw"
    target_table = "postgres_table"

    df = pl.read_database_uri(
        f"SELECT * FROM {source_key} LIMIT 100000",
        postgres.connection_uri,
        engine="connectorx",
    )

    context.log.info(
        f"Loaded {df.height:,} rows from PostgreSQL → {target_bucket}/{target_table}"
    )

    arrow_data = df.to_arrow()  # type: ignore
    opts = s3.delta_storage_options
    uri = f"s3://{target_bucket}/{target_table}"
    try:
        (
            DeltaTable(uri, storage_options=opts)
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
            uri,
            mode="error",
            storage_options=opts,
        )
    except Exception as e:
        raise e
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_duckdb(
    context: dg.AssetExecutionContext,
    duckdb_source: DuckDBResource,
) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "duckdb_table"

    with duckdb.connect(duckdb_source.path, read_only=duckdb_source.read_only) as con:
        df: pl.DataFrame = con.execute("SELECT * FROM source_table").pl()

    context.log.info(
        f"Loaded {df.height:,} rows from DuckDB → {target_bucket}/{target_table}"
    )

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# HTTP (REST / JSON API)
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_http(
    context: dg.AssetExecutionContext,
    http: HttpResource,
) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "http_table"

    records = http.get("/data")

    df = pl.DataFrame(records)
    context.log.info(
        f"Loaded {df.height:,} rows from HTTP → {target_bucket}/{target_table}"
    )

    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_csv(
    context: dg.AssetExecutionContext,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "csv_table"
    source_key = "raw/upstream/data.csv"

    fs = s3.get_filesystem()
    with fs.open(source_key, mode="rb") as f:  # type: ignore[reportUnknownMemberType]
        df = pl.read_csv(cast(IO[bytes], f))

    context.log.info(
        f"Loaded {df.height:,} rows from CSV → {target_bucket}/{target_table}"
    )

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_parquet(
    context: dg.AssetExecutionContext,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "parquet_table"
    source_key = "raw/upstream/data.parquet"

    fs = s3.get_filesystem()
    with fs.open(source_key, mode="rb") as f:  # type: ignore[reportUnknownMemberType]
        df = pl.read_parquet(cast(IO[bytes], f))

    context.log.info(
        f"Loaded {df.height:,} rows from Parquet → {target_bucket}/{target_table}"
    )

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# Excel (XLSX)
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_xlsx(
    context: dg.AssetExecutionContext,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "xlsx_table"
    source_key = "raw/upstream/source.xlsx"
    sheet_name = "Sheet1"

    fs = s3.get_filesystem()
    with fs.open(source_key, mode="rb") as f:  # type: ignore[reportUnknownMemberType]
        df = pl.read_excel(cast(IO[bytes], f), sheet_name=sheet_name)

    context.log.info(
        f"Loaded {df.height:,} rows from XLSX → {target_bucket}/{target_table}"
    )

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# JSON (newline-delimited or single document)
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_json(
    context: dg.AssetExecutionContext,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "json_table"
    source_key = "raw/upstream/source.json"

    fs = s3.get_filesystem()
    # use pl.read_ndjson for newline-delimited JSON (NDJSON / JSON-L)
    with fs.open(source_key, mode="rb") as f:  # type: ignore[reportUnknownMemberType]
        df = pl.read_json(cast(IOBase, f))

    context.log.info(
        f"Loaded {df.height:,} rows from JSON → {target_bucket}/{target_table}"
    )

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )
