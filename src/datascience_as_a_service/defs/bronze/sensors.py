"""Sensors for bronze assets."""

from datetime import UTC, datetime

import boto3
import dagster as dg

from ..resources import S3Resource

WATCHED_BUCKET = "raw"
_EXTENSION_TO_ASSET = {
    "csv": "ingest_csv",
    "parquet": "ingest_parquet",
    "xlsx": "ingest_xlsx",
    "json": "ingest_json",
    "ndjson": "ingest_ndjson",
    "jsonl": "ingest_ndjson",
}


@dg.sensor(  # type: ignore
    target=list(set(_EXTENSION_TO_ASSET.values())),
    minimum_interval_seconds=60,
)
def new_object_sensor(
    context: dg.SensorEvaluationContext,
    s3: S3Resource,
) -> dg.SensorResult:
    """Fires a run whenever a new object lands in `s3://raw`.

    State is kept two ways:
      - the sensor cursor holds the max `LastModified` seen so far, so a
        restart or a slow/duplicate tick never reprocesses old objects.
      - each `RunRequest.run_key` is derived from the object's key, so even
        if this sensor's own bookkeeping somehow re-reports the same
        object, Dagster itself won't launch a second run for it.

    On the very first tick (no cursor yet) nothing is materialized — objects
    already sitting in the bucket predate the sensor, so this only seeds a
    baseline and starts reacting to whatever lands *after* that.
    """

    client = boto3.client(  # type: ignore
        "s3",
        endpoint_url=s3.endpoint_url,
        aws_access_key_id=s3.access_key,
        aws_secret_access_key=s3.secret_key,
        region_name=s3.region,
    )

    since = context.cursor
    is_first_tick = since is None
    latest_seen = since
    new_keys: list[str] = []

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=WATCHED_BUCKET):
        for obj in page.get("Contents", []):
            last_modified = obj["LastModified"].isoformat()  # type: ignore
            if since is not None and last_modified > since:
                new_keys.append(obj["Key"])  # type: ignore
            if latest_seen is None or last_modified > latest_seen:
                latest_seen = last_modified

    if is_first_tick:
        return dg.SensorResult(
            skip_reason=(
                "First tick — establishing baseline, not backfilling existing objects."
            ),
            cursor=latest_seen or datetime.now(UTC).isoformat(),
        )

    if not new_keys:
        return dg.SensorResult(
            skip_reason=f"No new objects in s3://{WATCHED_BUCKET} since {since}"
        )

    context.log.info(f"New objects in s3://{WATCHED_BUCKET}: {new_keys}")

    run_requests: list[dg.RunRequest] = []
    for key in new_keys:
        extension = key.rsplit(".", 1)[-1].lower() if "." in key else ""
        asset_name = _EXTENSION_TO_ASSET.get(extension)
        if asset_name is None:
            context.log.warning(
                f"No ingestion asset for extension '{extension}': {key}"
            )
            continue
        run_requests.append(
            dg.RunRequest(
                run_key=f"{WATCHED_BUCKET}/{key}",
                asset_selection=[dg.AssetKey(asset_name)],
                tags={"source_key": key},
            )
        )

    return dg.SensorResult(run_requests=run_requests, cursor=latest_seen)
