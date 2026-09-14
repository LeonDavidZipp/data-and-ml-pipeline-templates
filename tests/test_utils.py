from unittest.mock import MagicMock, patch

import polars as pl
from deltalake.exceptions import TableNotFoundError

from src.datascience_as_a_service.utils import upsert_deltatable


class TestUpsertDeltatable:
    def setup_method(self) -> None:
        self.df = pl.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        self.opts = {"endpoint_url": "http://minio:9000"}
        self.uri = "s3://bucket/table"

    @patch.object(pl.DataFrame, "write_delta")
    def test_merge_existing_table(self, mock_write_delta: MagicMock) -> None:
        mock_merger = MagicMock()
        mock_write_delta.return_value = mock_merger
        mock_merger.when_matched_update_all.return_value = mock_merger
        mock_merger.when_not_matched_insert_all.return_value = mock_merger

        upsert_deltatable(self.uri, self.df, self.opts, predicate="t.id = s.id")

        mock_write_delta.assert_called_once_with(
            self.uri,
            mode="merge",
            storage_options=self.opts,
            delta_merge_options={
                "predicate": "t.id = s.id",
                "source_alias": "s",
                "target_alias": "t",
            },
        )
        mock_merger.execute.assert_called_once()

    @patch.object(pl.DataFrame, "write_delta")
    def test_creates_table_when_not_found(self, mock_write_delta: MagicMock) -> None:
        mock_write_delta.side_effect = [TableNotFoundError("not found"), None]

        upsert_deltatable(self.uri, self.df, self.opts, predicate="t.id = s.id")

        assert mock_write_delta.call_count == 2
        mock_write_delta.assert_any_call(
            self.uri, mode="error", storage_options=self.opts
        )

    @patch.object(pl.DataFrame, "write_delta")
    def test_custom_aliases(self, mock_write_delta: MagicMock) -> None:
        mock_merger = MagicMock()
        mock_write_delta.return_value = mock_merger
        mock_merger.when_matched_update_all.return_value = mock_merger
        mock_merger.when_not_matched_insert_all.return_value = mock_merger

        upsert_deltatable(
            self.uri,
            self.df,
            self.opts,
            predicate="target.id = source.id",
            target_alias="target",
            source_alias="source",
        )

        call_kwargs = mock_write_delta.call_args
        merge_opts = call_kwargs.kwargs["delta_merge_options"]
        assert merge_opts["target_alias"] == "target"
        assert merge_opts["source_alias"] == "source"

    @patch.object(pl.DataFrame, "write_delta")
    def test_reraises_unexpected_errors(self, mock_write_delta: MagicMock) -> None:
        mock_write_delta.side_effect = RuntimeError("unexpected")

        try:
            upsert_deltatable(self.uri, self.df, self.opts, predicate="t.id = s.id")
            assert False, "Should have raised"
        except RuntimeError as e:
            assert str(e) == "unexpected"
