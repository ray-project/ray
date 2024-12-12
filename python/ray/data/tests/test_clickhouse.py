import re
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
            table="default.table_name",
            dsn="clickhouse://user:password@localhost:8123/default",
            columns=["column1", "column2"],
            order_by=(["column1"], False),
            client_settings={"setting1": "value1"},
            client_kwargs={"arg1": "value1"},
        )
        datasource._client = mock_clickhouse_client
        return datasource

    def test_init(self, datasource):
        # Test query generation with columns, filters, and order_by
        expected_query = (
            "SELECT column1, column2 FROM default.table_name ORDER BY column1"
        )
        assert datasource._query == expected_query

    def test_estimate_inmemory_data_size(self, datasource):
        mock_client = mock.MagicMock()
        datasource._init_client = MagicMock(return_value=mock_client)
        mock_client.query.return_value.result_rows = [[12345]]
        size = datasource.estimate_inmemory_data_size()
        assert size == 12345
        mock_client.query.assert_called_once_with(
            f"SELECT SUM(byteSize(*)) AS estimate FROM ({datasource._query})"
        )

    @pytest.mark.parametrize(
        "limit_row_count, offset_row_count, expected_query",
        [
            (
                10,
                0,
                """
                SELECT column1, column2 FROM default.table_name ORDER BY column1
                FETCH FIRST 10 ROWS ONLY
                """.strip(),
            ),
            (
                1,
                0,
                """
                SELECT column1, column2 FROM default.table_name ORDER BY column1
                FETCH FIRST 1 ROW ONLY
                """.strip(),
            ),
            (
                10,
                5,
                """
                SELECT column1, column2 FROM default.table_name ORDER BY column1
                OFFSET 5 ROWS
                FETCH NEXT 10 ROWS ONLY
                """.strip(),
            ),
            (
                1,
                1,
                """
                SELECT column1, column2 FROM default.table_name ORDER BY column1
                OFFSET 1 ROW
                FETCH NEXT 1 ROW ONLY
                """.strip(),
            ),
        ],
    )
    def test_build_block_query(
        self, datasource, limit_row_count, offset_row_count, expected_query
    ):
        generated_query = datasource._build_block_query(
            limit_row_count, offset_row_count
        )
        clean_generated_query = re.sub(r"\s+", " ", generated_query.strip())
        clean_expected_query = re.sub(r"\s+", " ", expected_query.strip())
        assert clean_generated_query == clean_expected_query

    @pytest.mark.parametrize(
        "columns, expected_query_part",
        [
            (
                [
                    "field1",
                ],
                "SELECT field1 FROM default.table_name",
            ),
            (["field1", "field2"], "SELECT field1, field2 FROM default.table_name"),
            (None, "SELECT * FROM default.table_name"),
        ],
    )
    def test_generate_query_columns(self, datasource, columns, expected_query_part):
        datasource._columns = columns
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

    @pytest.mark.parametrize(
        "query_params, expected_query",
        [
            (
                {},
                "SELECT * FROM default.table_name",
            ),
            (
                {
                    "columns": ["field1"],
                },
                "SELECT field1 FROM default.table_name",
            ),
            (
                {
                    "columns": ["field1"],
                    "order_by": (["field1"], False),
                },
                "SELECT field1 FROM default.table_name ORDER BY field1",
            ),
            (
                {
                    "columns": ["field1", "field2"],
                    "order_by": (["field1"], True),
                },
                "SELECT field1, field2 FROM default.table_name ORDER BY field1 DESC",
            ),
            (
                {
                    "columns": ["field1", "field2", "field3"],
                    "order_by": (["field1", "field2"], False),
                },
                "SELECT field1, field2, field3 FROM default.table_name "
                "ORDER BY (field1, field2)",
            ),
            (
                {
                    "columns": ["field1", "field2", "field3"],
                    "order_by": (["field1", "field2"], True),
                },
                "SELECT field1, field2, field3 FROM default.table_name "
                "ORDER BY (field1, field2) DESC",
            ),
            (
                {
                    "columns": ["field1", "field2", "field3"],
                    "order_by": (["field1", "field2", "field3"], True),
                },
                "SELECT field1, field2, field3 FROM default.table_name "
                "ORDER BY (field1, field2, field3) DESC",
            ),
        ],
    )
    def test_generate_query_full(self, datasource, query_params, expected_query):
        datasource._columns = query_params.get("columns")
        datasource._order_by = query_params.get("order_by")
        generated_query = datasource._generate_query()
        assert expected_query == generated_query

    @pytest.mark.parametrize("parallelism", [1, 2, 3, 4])
    def test_get_read_tasks_ordered_table(self, datasource, parallelism):
        batch1 = pa.record_batch([pa.array([1, 2, 3, 4, 5, 6, 7, 8])], names=["field1"])
        batch2 = pa.record_batch(
            [pa.array([9, 10, 11, 12, 13, 14, 15, 16])], names=["field1"]
        )
        mock_stream = MagicMock()
        mock_client = mock.MagicMock()
        mock_client.query_arrow_stream.return_value.__enter__.return_value = mock_stream
        mock_stream.__iter__.return_value = [batch1, batch2]
        datasource.MIN_ROWS_PER_READ_TASK = 4
        datasource._init_client = MagicMock(return_value=mock_client)
        datasource._get_estimate_count = MagicMock(return_value=16)
        datasource._get_sampled_estimates = MagicMock(return_value=(100, batch1.schema))
        read_tasks = datasource.get_read_tasks(parallelism)
        expected_num_tasks = parallelism
        assert len(read_tasks) == expected_num_tasks
        total_rows = sum(batch.num_rows for batch in [batch1, batch2])
        rows_per_task = total_rows // parallelism
        extra_rows = total_rows % parallelism
        for i, read_task in enumerate(read_tasks):
            expected_rows = rows_per_task + (1 if i < extra_rows else 0)
            assert read_task.metadata.num_rows == expected_rows

    @pytest.mark.parametrize("parallelism", [1, 4])
    def test_get_read_tasks_no_ordering(self, datasource, parallelism):
        datasource._order_by = None
        batch1 = pa.record_batch([pa.array([1, 2, 3, 4, 5, 6, 7, 8])], names=["field2"])
        batch2 = pa.record_batch(
            [pa.array([9, 10, 11, 12, 13, 14, 15, 16])], names=["field2"]
        )
        mock_stream = MagicMock()
        mock_client = mock.MagicMock()
        mock_client.query_arrow_stream.return_value.__enter__.return_value = mock_stream
        mock_stream.__iter__.return_value = [batch1, batch2]
        datasource.MIN_ROWS_PER_READ_TASK = 4
        datasource._init_client = MagicMock(return_value=mock_client)
        datasource._get_estimate_count = MagicMock(return_value=16)
        datasource._get_sampled_estimates = MagicMock(return_value=(100, batch1.schema))
        read_tasks = datasource.get_read_tasks(parallelism)
        assert len(read_tasks) == 1
        for i, read_task in enumerate(read_tasks):
            assert read_task.metadata.num_rows == 16

    def test_get_read_tasks_no_batches(self, datasource, mock_clickhouse_client):
        mock_reader = mock.MagicMock()
        mock_reader.__iter__.return_value = iter([])
        datasource._init_client = MagicMock(return_value=mock_clickhouse_client)
        datasource._get_estimate_count = MagicMock(return_value=0)
        mock_block_accessor = mock.MagicMock()
        datasource._get_sampled_estimates = MagicMock(return_value=(0, None))
        datasource._get_sample_block = MagicMock(return_value=mock_block_accessor)
        read_tasks = datasource.get_read_tasks(parallelism=2)
        assert len(read_tasks) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
