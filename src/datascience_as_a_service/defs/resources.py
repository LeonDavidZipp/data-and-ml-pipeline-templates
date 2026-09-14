from __future__ import annotations

from typing import Any

import dagster as dg
import s3fs  # type: ignore
from dagster_mlflow import mlflow_tracking  # type: ignore[reportUnknownVariableType]

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

    def get(
        self, path: str, params: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
        import httpx

        headers: dict[str, str] = {"Authorization": f"Bearer {self.token}"}
        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
            response = client.get(path, params=params or {}, headers=headers)
            response.raise_for_status()
            return response.json()


# ---------------------------------------------------------------------------
# S3 / RustFS
# ---------------------------------------------------------------------------


class S3Resource(dg.ConfigurableResource[Any]):
    endpoint_url: str = dg.EnvVar("AWS_ENDPOINT_URL")
    access_key: str = dg.EnvVar("AWS_ACCESS_KEY_ID")
    secret_key: str = dg.EnvVar("AWS_SECRET_ACCESS_KEY")
    region: str = dg.EnvVar("AWS_REGION")
    allow_http: str = "false"

    @property
    def delta_storage_options(self) -> dict[str, str]:
        return {
            "endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_region": self.region,
            "aws_allow_http": self.allow_http,
        }

    def get_filesystem(self) -> s3fs.S3FileSystem:
        return s3fs.S3FileSystem(
            anon=False,
            key=self.access_key,
            secret=self.secret_key,
            client_kwargs={
                "region_name": self.region,
                "endpoint_url": self.endpoint_url,
            },
        )


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
            "mlflow": mlflow_tracking,
        },
    )
