from unittest.mock import MagicMock

import pytest

from src.datascience_as_a_service.defs.resources import (
    DuckDBResource,
    HttpResource,
    S3Resource,
    SqlResource,
)


@pytest.fixture
def mock_sql_resource() -> SqlResource:
    return SqlResource(
        connection_uri="postgresql://testuser:testpass@localhost:5432/testdb",
    )


@pytest.fixture
def mock_duckdb_resource() -> DuckDBResource:
    return DuckDBResource(
        path="/path/to/mock.duckdb",
        read_only=True,
    )


@pytest.fixture
def mock_http_resource() -> MagicMock:
    resource = MagicMock(spec=HttpResource)
    resource.base_url = "https://api.mock.com"
    resource.token = "mocktoken"
    resource.timeout = 30
    resource.get.return_value = []
    return resource


@pytest.fixture
def mock_s3_resource() -> MagicMock:
    resource = MagicMock(spec=S3Resource)
    resource.endpoint_url = "https://mock-bucket.s3.amazonaws.com"
    resource.access_key = "mock-access-key"
    resource.secret_key = "mock-secret-key"
    resource.region = "us-east-1"
    resource.allow_http = "true"
    resource.delta_storage_options = {
        "endpoint_url": "https://mock-bucket.s3.amazonaws.com",
        "aws_access_key_id": "mock-access-key",
        "aws_secret_access_key": "mock-secret-key",
        "aws_region": "us-east-1",
        "aws_allow_http": "true",
    }
    resource.get_filesystem.return_value = MagicMock()
    return resource
