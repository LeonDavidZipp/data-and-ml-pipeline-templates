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

import dagster as dg
import duckdb
import httpx
import polars as pl
from deltalake import DeltaTable, write_deltalake  # type: ignore


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_postgres(context: dg.AssetExecutionContext) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "postgres_table"

    connection_uri = "postgresql://dp_pg_user:supersecret@postgres:5432/dp_db"
    df = pl.read_database_uri(
        "SELECT * FROM source_table LIMIT 100000",
        connection_uri,
        engine="connectorx",
    )

    context.log.info(f"Loaded {df.height:,} rows from PostgreSQL → {target_bucket}/{target_table}")

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(value=None, metadata={"row_count": dg.MetadataValue.int(df.height)})


# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_duckdb(context: dg.AssetExecutionContext) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "duckdb_table"

    con = duckdb.connect("/path/to/source.duckdb", read_only=True)
    df: pl.DataFrame = con.execute("SELECT * FROM source_table").pl()
    con.close()

    context.log.info(f"Loaded {df.height:,} rows from DuckDB → {target_bucket}/{target_table}")

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(value=None, metadata={"row_count": dg.MetadataValue.int(df.height)})


# ---------------------------------------------------------------------------
# HTTP (REST / JSON API)
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_http(context: dg.AssetExecutionContext) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "http_table"

    url = "https://api.example.com/data"
    params: dict[str, str] = {}
    headers: dict[str, str] = {"Authorization": "Bearer <token>"}

    with httpx.Client(timeout=30) as client:
        response = client.get(url, params=params, headers=headers)
        response.raise_for_status()
        records = response.json() 

    df = pl.DataFrame(records)
    context.log.info(f"Loaded {df.height:,} rows from HTTP → {target_bucket}/{target_table}")

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(value=None, metadata={"row_count": dg.MetadataValue.int(df.height)})


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_csv(context: dg.AssetExecutionContext) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "csv_table"

    source_path = "s3://raw/upstream/data.csv"  # local path or s3:// URI (needs s3fs)
    df = pl.read_csv(source_path)

    context.log.info(f"Loaded {df.height:,} rows from CSV → {target_bucket}/{target_table}")

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(value=None, metadata={"row_count": dg.MetadataValue.int(df.height)})


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "parquet_table"

    source_path = "s3://raw/upstream/data.parquet"  # local path or s3:// URI (needs s3fs)
    df = pl.read_parquet(source_path)

    context.log.info(f"Loaded {df.height:,} rows from Parquet → {target_bucket}/{target_table}")

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(value=None, metadata={"row_count": dg.MetadataValue.int(df.height)})


# ---------------------------------------------------------------------------
# Excel (XLSX)
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_xlsx(context: dg.AssetExecutionContext) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "xlsx_table"

    source_path = "/path/to/source.xlsx"
    sheet_name = "Sheet1"  # name of the sheet to read
    df = pl.read_excel(source_path, sheet_name=sheet_name)

    context.log.info(f"Loaded {df.height:,} rows from XLSX → {target_bucket}/{target_table}")

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(value=None, metadata={"row_count": dg.MetadataValue.int(df.height)})


# ---------------------------------------------------------------------------
# JSON (newline-delimited or single document)
# ---------------------------------------------------------------------------


@dg.asset(group_name="raw")
def ingest_json(context: dg.AssetExecutionContext) -> dg.MaterializeResult[None]:
    target_bucket = "raw"
    target_table = "json_table"

    source_path = "/path/to/source.json"
    # use pl.read_ndjson for newline-delimited JSON (NDJSON / JSON-L)
    df = pl.read_json(source_path)

    context.log.info(f"Loaded {df.height:,} rows from JSON → {target_bucket}/{target_table}")

    # TODO: write df to Delta at s3://{target_bucket}/{target_table}
    return dg.MaterializeResult(value=None, metadata={"row_count": dg.MetadataValue.int(df.height)})
