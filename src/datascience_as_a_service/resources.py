from __future__ import annotations

from typing import Any

import dagster as dg


class PostgresResource(dg.ConfigurableResource[Any]):
    host: str = "postgres"
    port: int = 5432
    dbname: str = dg.EnvVar("POSTGRES_DB")
    user: str = dg.EnvVar("POSTGRES_USER")
    password: str = dg.EnvVar("POSTGRES_PASSWORD")

    @property
    def connection_uri(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"


class DuckDBResource(dg.ConfigurableResource[Any]):
    path: str = "/path/to/source.duckdb"
    read_only: bool = True


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


class S3Resource(dg.ConfigurableResource[Any]):
    endpoint_url: str = dg.EnvVar("MLFLOW_S3_ENDPOINT_URL")
    access_key: str = dg.EnvVar("AWS_ACCESS_KEY_ID")
    secret_key: str = dg.EnvVar("AWS_SECRET_ACCESS_KEY")
