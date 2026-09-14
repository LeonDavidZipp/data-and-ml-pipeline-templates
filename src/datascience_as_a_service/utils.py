import logging
import random
import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

import polars as pl
from deltalake.exceptions import TableNotFoundError

P = ParamSpec("P")
T = TypeVar("T")

logger = logging.getLogger(__name__)


def retry(
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Decorator that retries a function with exponential backoff and jitter.

    Only retries the exception types listed in `exceptions`; anything else
    propagates immediately. Re-raises the last exception once `max_attempts`
    is exhausted.

    Args:
        exceptions: exception types that should trigger a retry.
        max_attempts: total number of attempts, including the first.
        base_delay: seconds to wait after the first failure; doubles each
            subsequent attempt.
        max_delay: upper bound on the backoff delay.
    """

    def decorator(fn: Callable[P, T]) -> Callable[P, T]:
        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            attempt = 0
            while True:
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    attempt += 1
                    if attempt >= max_attempts:
                        raise
                    delay = min(base_delay * 2 ** (attempt - 1), max_delay)
                    delay += random.uniform(0, delay * 0.1)
                    logger.warning(
                        "%s failed (attempt %d/%d): %s — retrying in %.1fs",
                        fn.__qualname__,
                        attempt,
                        max_attempts,
                        exc,
                        delay,
                    )
                    time.sleep(delay)

        return wrapper

    return decorator


def upsert_deltatable(
    table_uri: str,
    df: pl.DataFrame,
    storage_options: dict[str, str],
    predicate: str,
    target_alias: str = "t",
    source_alias: str = "s",
) -> None:
    """
    Try to upsert a DeltaTable with the given DataFrame. If the table does not
    exist, it will be created. If it does exist, the DataFrame will be merged with
    the existing data using the provided predicate.

    Args:
        table_uri (str): The URI of the DeltaTable (e.g., "s3://<bucket>/<table>").
        df (pl.DataFrame): The DataFrame to upsert.
        storage_options (dict[str, str]): Storage options for accessing the DeltaTable.
        predicate (str): The predicate for merging the DataFrame with the existing data.
        target_alias (str): The alias for the target table in the merge operation.
        source_alias (str): The alias for the source DataFrame in the merge operation.
    """
    try:
        (
            df.write_delta(
                table_uri,
                mode="merge",
                storage_options=storage_options,
                delta_merge_options={
                    "predicate": predicate,
                    "source_alias": source_alias,
                    "target_alias": target_alias,
                },
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    except TableNotFoundError:
        df.write_delta(
            table_uri,
            mode="error",
            storage_options=storage_options,
        )
