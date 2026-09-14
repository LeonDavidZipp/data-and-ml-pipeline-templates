"""Raw ingestion assets — one asset per source type.

Each asset reads from an external source and will materialise the result as a
Delta table.  Two variables are defined at the top of every asset body:

    target_bucket : str  – S3 bucket where the Delta table will be written
    target_table  : str  – table name used as the S3 key prefix / table path

Additional packages required per asset (install with ``uv add <pkg>``):
    sql       → polars, connectorx (plus a DB-API driver for your dialect,
                e.g. psycopg2-binary for postgresql://, pymysql for mysql://)
    duckdb    → duckdb  (already in deps), polars
    http      → httpx, polars
    csv       → polars (native cloud read via `scan_csv`, no filesystem client)
    parquet   → polars (native cloud read via `read_parquet`)
    ndjson    → polars (native cloud read via `read_ndjson`)
    json      → polars, boto3 (single JSON documents have no native cloud
                reader in polars, so raw bytes are fetched via S3 API)
    xlsx      → polars, xlsx2csv, boto3 (same reason as json)

`S3Resource` only holds credentials/config — it doesn't wrap a specific
client. Where polars can read straight from an `s3://` URI (csv/parquet/
ndjson) we pass `s3.delta_storage_options` directly and skip a client
entirely; where it can't (json/xlsx) `_get_s3_bytes()` below fetches the
object via `boto3`. Swap either for whatever fits your source.
"""

import dagster as dg
import duckdb
import polars as pl

from datascience_as_a_service.defs.resources import (
    DuckDBResource,
    HttpResource,
    S3Resource,
    SqlResource,
)
from datascience_as_a_service.utils import upsert_deltatable


def _get_s3_bytes(s3: S3Resource, bucket: str, key: str) -> bytes:
    import boto3

    client = boto3.client(  # type: ignore
        "s3",
        endpoint_url=s3.endpoint_url,
        aws_access_key_id=s3.access_key,
        aws_secret_access_key=s3.secret_key,
        region_name=s3.region,
    )
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


# ---------------------------------------------------------------------------
# SQL (dialect-agnostic — driven by SqlResource.connection_uri)
# ---------------------------------------------------------------------------


@dg.asset(group_name="bronze")
def ingest_sql(
    context: dg.AssetExecutionContext,
    sql: SqlResource,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    source_key = "changeme"
    target_bucket = "bronze"
    target_table = "sql_table"

    df = pl.read_database_uri(
        f"SELECT * FROM {source_key} LIMIT 100000",
        sql.connection_uri,
        engine="connectorx",
    )

    context.log.info(
        f"Loaded {df.height:,} rows from SQL → {target_bucket}/{target_table}"
    )

    opts = s3.delta_storage_options
    uri = f"s3://{target_bucket}/{target_table}"
    upsert_deltatable(
        uri, df, opts, predicate="t.id = s.id", target_alias="t", source_alias="s"
    )
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------


@dg.asset(group_name="bronze")
def ingest_duckdb(
    context: dg.AssetExecutionContext,
    duckdb_source: DuckDBResource,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    target_bucket = "bronze"
    target_table = "duckdb_table"

    with duckdb.connect(duckdb_source.path, read_only=duckdb_source.read_only) as con:
        df: pl.DataFrame = con.execute("SELECT * FROM source_table").pl()

    context.log.info(
        f"Loaded {df.height:,} rows from DuckDB → {target_bucket}/{target_table}"
    )

    uri = f"s3://{target_bucket}/{target_table}"
    opts = s3.delta_storage_options
    upsert_deltatable(
        uri, df, opts, predicate="t.id = s.id", target_alias="t", source_alias="s"
    )
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# HTTP (REST / JSON API)
# ---------------------------------------------------------------------------


@dg.asset(group_name="bronze")
def ingest_http(
    context: dg.AssetExecutionContext,
    http: HttpResource,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    target_bucket = "bronze"
    target_table = "http_table"

    records = http.get("/data")

    df = pl.DataFrame(records)
    context.log.info(
        f"Loaded {df.height:,} rows from HTTP → {target_bucket}/{target_table}"
    )

    uri = f"s3://{target_bucket}/{target_table}"
    opts = s3.delta_storage_options
    upsert_deltatable(
        uri, df, opts, predicate="t.id = s.id", target_alias="t", source_alias="s"
    )
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


class CsvIngestConfig(dg.Config):
    source_bucket: str = "raw"
    source_key: str = "changeme.csv"


@dg.asset(group_name="bronze")
def ingest_csv(
    context: dg.AssetExecutionContext,
    config: CsvIngestConfig,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    target_bucket = "bronze"
    target_table = "csv_table"

    opts = s3.delta_storage_options
    source_uri = f"s3://{config.source_bucket}/{config.source_key}"
    df = pl.scan_csv(source_uri, storage_options=opts).collect()

    context.log.info(
        f"Loaded {df.height:,} rows from CSV → {target_bucket}/{target_table}"
    )

    uri = f"s3://{target_bucket}/{target_table}"
    upsert_deltatable(
        uri, df, opts, predicate="t.id = s.id", target_alias="t", source_alias="s"
    )
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------


class ParquetIngestConfig(dg.Config):
    source_bucket: str = "raw"
    source_key: str = "changeme.parquet"


@dg.asset(group_name="bronze")
def ingest_parquet(
    context: dg.AssetExecutionContext,
    config: ParquetIngestConfig,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    target_bucket = "bronze"
    target_table = "parquet_table"

    opts = s3.delta_storage_options
    source_uri = f"s3://{config.source_bucket}/{config.source_key}"
    df = pl.read_parquet(source_uri, storage_options=opts)

    context.log.info(
        f"Loaded {df.height:,} rows from Parquet → {target_bucket}/{target_table}"
    )

    uri = f"s3://{target_bucket}/{target_table}"
    upsert_deltatable(
        uri, df, opts, predicate="t.id = s.id", target_alias="t", source_alias="s"
    )
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# Excel (XLSX)
# ---------------------------------------------------------------------------


class XlsxIngestConfig(dg.Config):
    source_bucket: str = "raw"
    source_key: str = "changeme.xlsx"
    sheet_name: str = "Sheet1"


@dg.asset(group_name="bronze")
def ingest_xlsx(
    context: dg.AssetExecutionContext,
    config: XlsxIngestConfig,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    target_bucket = "bronze"
    target_table = "xlsx_table"

    data = _get_s3_bytes(s3, config.source_bucket, config.source_key)
    df = pl.read_excel(data, sheet_name=config.sheet_name)

    context.log.info(
        f"Loaded {df.height:,} rows from XLSX → {target_bucket}/{target_table}"
    )

    uri = f"s3://{target_bucket}/{target_table}"
    opts = s3.delta_storage_options
    upsert_deltatable(
        uri, df, opts, predicate="t.id = s.id", target_alias="t", source_alias="s"
    )
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


# ---------------------------------------------------------------------------
# JSON (newline-delimited or single document)
# ---------------------------------------------------------------------------


class JsonIngestConfig(dg.Config):
    source_bucket: str = "raw"
    source_key: str = "changeme.json"


@dg.asset(group_name="bronze")
def ingest_json(
    context: dg.AssetExecutionContext,
    config: JsonIngestConfig,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    target_bucket = "bronze"
    target_table = "json_table"

    data = _get_s3_bytes(s3, config.source_bucket, config.source_key)
    df = pl.read_json(data)

    context.log.info(
        f"Loaded {df.height:,} rows from JSON → {target_bucket}/{target_table}"
    )

    uri = f"s3://{target_bucket}/{target_table}"
    opts = s3.delta_storage_options
    upsert_deltatable(
        uri, df, opts, predicate="t.id = s.id", target_alias="t", source_alias="s"
    )
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )


class NdjsonIngestConfig(dg.Config):
    source_bucket: str = "raw"
    source_key: str = "changeme.json"  # or .ndjson or .jsonl


@dg.asset(group_name="bronze")
def ingest_ndjson(
    context: dg.AssetExecutionContext,
    config: NdjsonIngestConfig,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    target_bucket = "bronze"
    target_table = "ndjson_table"

    opts = s3.delta_storage_options
    source_uri = f"s3://{config.source_bucket}/{config.source_key}"
    df = pl.read_ndjson(source_uri, storage_options=opts)

    context.log.info(
        f"Loaded {df.height:,} rows from NDJSON → {target_bucket}/{target_table}"
    )

    uri = f"s3://{target_bucket}/{target_table}"
    upsert_deltatable(
        uri, df, opts, predicate="t.id = s.id", target_alias="t", source_alias="s"
    )
    return dg.MaterializeResult(
        value=None, metadata={"row_count": dg.MetadataValue.int(df.height)}
    )
