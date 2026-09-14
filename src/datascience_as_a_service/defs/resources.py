from __future__ import annotations

from typing import Any

import dagster as dg
import httpx
from dagster_mlflow import mlflow_tracking  # type: ignore[reportUnknownVariableType]

from datascience_as_a_service.utils import retry

# ---------------------------------------------------------------------------
# SQL (dialect-agnostic)
# ---------------------------------------------------------------------------


class SqlResource(dg.ConfigurableResource[Any]):
    """Generic SQL data source, configured by connection string.

    Dialect-agnostic: the scheme of ``connection_uri`` (e.g. ``postgresql://``,
    ``mysql://``, ``mssql://``) determines what it connects to.
    """

    connection_uri: str = dg.EnvVar("SQL_SOURCE_CONNECTION_URI")


# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------


class DuckDBResource(dg.ConfigurableResource[Any]):
    path: str = "/path/to/source.duckdb"
    read_only: bool = True


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


class HttpResource(dg.ConfigurableResource[Any]):
    base_url: str = "https://api.example.com"
    token: str = dg.EnvVar("HTTP_API_TOKEN")
    timeout: int = 30

    @retry(exceptions=(httpx.TransportError,))
    def get(
        self, path: str, params: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
        headers: dict[str, str] = {"Authorization": f"Bearer {self.token}"}
        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
            response = client.get(path, params=params or {}, headers=headers)
            response.raise_for_status()
            return response.json()


# ---------------------------------------------------------------------------
# S3 / RustFS
# ---------------------------------------------------------------------------


class S3Resource(dg.ConfigurableResource[Any]):
    """S3-compatible storage config. Bring your own client library.

    Only holds credentials/config plus the storage-options dict that
    `polars`/`deltalake` already understand natively — pass it straight to
    `pl.read_parquet`/`scan_csv`/`read_ndjson`/`write_delta` for a native,
    client-free cloud read. For formats with no native cloud reader (xlsx,
    single-document json), build a client from these fields yourself; see
    `_get_s3_bytes` (uses `boto3`) in `bronze/assets.py` for an example.
    """

    endpoint_url: str = dg.EnvVar("AWS_ENDPOINT_URL")
    access_key: str = dg.EnvVar("AWS_ACCESS_KEY_ID")
    secret_key: str = dg.EnvVar("AWS_SECRET_ACCESS_KEY")
    region: str = dg.EnvVar("AWS_REGION")
    allow_http: str = dg.EnvVar("AWS_ALLOW_HTTP")

    @property
    def delta_storage_options(self) -> dict[str, str]:
        return {
            "endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_region": self.region,
            "aws_allow_http": self.allow_http,
        }


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------


class AlertResource(dg.ConfigurableResource[Any]):
    """Posts a message to a webhook (Slack/Mattermost-compatible ``{"text": ...}``).

    Leave ``webhook_url`` empty to disable alerting without changing call sites.
    """

    webhook_url: str = dg.EnvVar("ALERT_WEBHOOK_URL")
    timeout: int = 10

    @retry(exceptions=(httpx.TransportError,))
    def notify(self, message: str) -> None:
        if not self.webhook_url:
            return

        httpx.post(self.webhook_url, json={"text": message}, timeout=self.timeout)


# ---------------------------------------------------------------------------
# Register all resources so load_from_defs_folder auto-discovers them
# ---------------------------------------------------------------------------


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "sql": SqlResource(),
            "duckdb_source": DuckDBResource(),
            "http": HttpResource(),
            "s3": S3Resource(),
            "alerts": AlertResource(),
            "mlflow": mlflow_tracking,
        },
    )
