from unittest import mock
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from ray.data._internal.datasource.clickhouse_datasource import ClickHouseDatasource


@pytest.fixture
def mock_clickhouse_client():
    client_mock = mock.MagicMock()
    client_mock.return_value = client_mock
    return client_mock


class TestClickHouseDatasource:
    """Tests for ClickHouseDatasource."""

    @pytest.fixture
    def datasource(self, mock_clickhouse_client):
        datasource = ClickHouseDatasource(
            entity="default.table_name",
            dsn="clickhouse://user:password@localhost:8123/default",
            columns=("column1", "column2"),
            filters={"column1": ("is", "value1"), "column2": ("greater", 10)},
            order_by=(["column1"], False),
            client_settings={"setting1": "value1"},
            client_kwargs={"arg1": "value1"},
        )
        datasource._client = mock_clickhouse_client
        return datasource

    def test_init(self, datasource):
        # Test query generation with columns, filters, and order_by
        expected_query = (
            "SELECT column1, column2 FROM default.table_name "
            "WHERE column1 = 'value1' AND column2 > 10 ORDER BY column1"
        )
        assert datasource._query == expected_query

    def test_estimate_inmemory_data_size(self, datasource):
        mock_client = mock.MagicMock()
        datasource._client = mock_client
        mock_client.query.return_value.result_rows = [[12345]]
        size = datasource.estimate_inmemory_data_size()
        assert size == 12345
        mock_client.query.assert_called_once_with(
            f"SELECT SUM(byteSize(*)) AS total_size FROM ({datasource._query})"
        )

    @pytest.mark.parametrize(
        "columns, expected_query_part",
        [
            (("field1",), "SELECT field1 FROM default.table_name"),
            (("field1", "field2"), "SELECT field1, field2 FROM default.table_name"),
            (None, "SELECT * FROM default.table_name"),
        ],
    )
    def test_generate_query_columns(self, datasource, columns, expected_query_part):
        datasource._columns = columns
        generated_query = datasource._generate_query()
        assert expected_query_part in generated_query

    @pytest.mark.parametrize(
        "filters, expected_query_part",
        [
            ({"field1": ("is", "value1")}, "field1 = 'value1'"),
            ({"field2": ("greater", 10)}, "field2 > 10"),
            ({"field3": ("not", None)}, "field3 IS NOT NULL"),
        ],
    )
    def test_generate_query_filters(self, datasource, filters, expected_query_part):
        datasource._filters = filters
        generated_query = datasource._generate_query()
        assert expected_query_part in generated_query

    @pytest.mark.parametrize(
        "order_by, expected_query_part",
        [
            ((["field1"], False), "ORDER BY field1"),
            ((["field2"], True), "ORDER BY field2 DESC"),
            ((["field1", "field2"], False), "ORDER BY (field1, field2)"),
        ],
    )
    def test_generate_query_with_order_by(
        self, datasource, order_by, expected_query_part
    ):
        datasource._order_by = order_by
        generated_query = datasource._generate_query()
        assert expected_query_part in generated_query

    @pytest.mark.parametrize("parallelism", [1, 2, 3, 4])
    def test_get_read_tasks(self, datasource, parallelism):
        batch1 = pa.record_batch([pa.array([1, 2])], names=["field1"])
        batch2 = pa.record_batch([pa.array([3, 4])], names=["field1"])
        mock_stream = MagicMock()
        datasource._client.query_arrow_stream.return_value.__enter__.return_value = (
            mock_stream
        )
        mock_stream.__iter__.return_value = [batch1, batch2]
        read_tasks = datasource.get_read_tasks(parallelism)
        expected_num_tasks = min(parallelism, len([batch1, batch2]))
        assert len(read_tasks) == expected_num_tasks
        for i, read_task in enumerate(read_tasks):
            expected_rows = sum(
                batch.num_rows for batch in [batch1, batch2][i::parallelism]
            )
            assert read_task.metadata.num_rows == expected_rows

    def test_get_read_tasks_no_batches(self, datasource, mock_clickhouse_client):
        # Mock an empty Arrow stream
        mock_reader = mock.MagicMock()
        mock_reader.__iter__.return_value = iter([])
        mock_clickhouse_client.query_arrow_stream.return_value = mock_reader
        read_tasks = datasource.get_read_tasks(parallelism=2)
        assert len(read_tasks) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
