from unittest.mock import MagicMock, patch

import polars as pl
from deltalake.exceptions import TableNotFoundError

from src.datascience_as_a_service.utils import upsert_deltatable


class TestUpsertDeltatable:
    def setup_method(self) -> None:
        self.df = pl.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        self.opts = {"endpoint_url": "http://minio:9000"}
        self.uri = "s3://bucket/table"

    @patch("src.datascience_as_a_service.utils.DeltaTable")
    def test_merge_existing_table(self, mock_dt_cls: MagicMock) -> None:
        mock_merge = MagicMock()
        mock_dt_cls.return_value.merge.return_value = mock_merge
        mock_merge.when_matched_update_all.return_value = mock_merge
        mock_merge.when_not_matched_insert_all.return_value = mock_merge

        upsert_deltatable(self.uri, self.df, self.opts, predicate="t.id = s.id")

        mock_dt_cls.assert_called_once_with(self.uri, storage_options=self.opts)
        mock_merge.execute.assert_called_once()

    @patch("src.datascience_as_a_service.utils.DeltaTable")
    def test_creates_table_when_not_found(self, mock_dt_cls: MagicMock) -> None:
        mock_dt_cls.side_effect = TableNotFoundError("not found")

        with patch.object(pl.DataFrame, "write_delta") as mock_write:
            upsert_deltatable(self.uri, self.df, self.opts, predicate="t.id = s.id")
            mock_write.assert_called_once_with(
                self.uri, mode="error", storage_options=self.opts
            )

    @patch("src.datascience_as_a_service.utils.DeltaTable")
    def test_custom_aliases(self, mock_dt_cls: MagicMock) -> None:
        mock_merge = MagicMock()
        mock_dt_cls.return_value.merge.return_value = mock_merge
        mock_merge.when_matched_update_all.return_value = mock_merge
        mock_merge.when_not_matched_insert_all.return_value = mock_merge

        upsert_deltatable(
            self.uri,
            self.df,
            self.opts,
            predicate="target.id = source.id",
            target_alias="target",
            source_alias="source",
        )

        call_kwargs = mock_dt_cls.return_value.merge.call_args
        assert call_kwargs.kwargs["target_alias"] == "target"
        assert call_kwargs.kwargs["source_alias"] == "source"

    @patch("src.datascience_as_a_service.utils.DeltaTable")
    def test_reraises_unexpected_errors(self, mock_dt_cls: MagicMock) -> None:
        mock_dt_cls.side_effect = RuntimeError("unexpected")

        try:
            upsert_deltatable(self.uri, self.df, self.opts, predicate="t.id = s.id")
            assert False, "Should have raised"
        except RuntimeError as e:
            assert str(e) == "unexpected"
