import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError


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
    arrow_data = df.to_arrow()  # type: ignore
    try:
        (
            DeltaTable(table_uri, storage_options=storage_options)
            .merge(
                arrow_data,  # type: ignore
                predicate=predicate,
                target_alias=target_alias,
                source_alias=source_alias,
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    except TableNotFoundError:
        df.write_delta(  # type: ignore
            table_uri,
            mode="error",
            storage_options=storage_options,
        )
    except Exception as e:
        raise e
