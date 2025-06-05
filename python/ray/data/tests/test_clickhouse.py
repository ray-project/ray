import re
from unittest import mock
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from clickhouse_connect.driver.summary import QuerySummary

import ray
from ray.data._internal.datasource.clickhouse_datasink import (
    ClickHouseDatasink,
    ClickHouseTableSettings,
    SinkMode,
)
from ray.data._internal.datasource.clickhouse_datasource import ClickHouseDatasource
from ray.data._internal.execution.interfaces.task_context import TaskContext


@pytest.fixture(autouse=True)
def patch_clickhouse_get_client():
    with patch("clickhouse_connect.get_client") as mock_factory:
        mock_instance = MagicMock()
        mock_instance.insert_arrow.return_value = QuerySummary({"written_rows": 3})
        mock_factory.return_value = mock_instance
        yield mock_instance


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
            client_kwargs={"client_name": "test-client"},
        )
        datasource._client = mock_clickhouse_client
        return datasource

    def test_init(self, datasource):
        expected_query = (
            "SELECT column1, column2 FROM default.table_name ORDER BY column1"
        )
        assert datasource._query == expected_query

    @mock.patch.object(ClickHouseDatasource, "_init_client")
    def test_init_with_filter(self, mock_init_client):
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client
        mock_client.query.return_value = MagicMock()
        ds_with_filter = ClickHouseDatasource(
            table="default.table_name",
            dsn="clickhouse://user:password@localhost:8123/default",
            columns=["column1", "column2"],
            filter="label = 2 AND text IS NOT NULL",
            order_by=(["column1"], False),
        )
        assert (
            ds_with_filter._query == "SELECT column1, column2 FROM default.table_name "
            "WHERE label = 2 AND text IS NOT NULL "
            "ORDER BY column1"
        )

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
                ["field1"],
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

    @mock.patch.object(ClickHouseDatasource, "_init_client")
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
            (
                {
                    "columns": None,
                    "filter": "label = 2",
                },
                "SELECT * FROM default.table_name WHERE label = 2",
            ),
            (
                {
                    "columns": ["field1", "field2"],
                    "filter": "label = 2 AND text IS NOT NULL",
                    "order_by": (["field1"], False),
                },
                "SELECT field1, field2 FROM default.table_name WHERE label = 2 AND "
                "text IS NOT NULL ORDER BY field1",
            ),
        ],
    )
    def test_generate_query_full(
        self, mock_init_client, datasource, query_params, expected_query
    ):
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client
        mock_client.query.return_value = MagicMock()
        datasource._columns = query_params.get("columns")
        datasource._filter = query_params.get("filter")
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

    @mock.patch.object(ClickHouseDatasource, "_init_client")
    @pytest.mark.parametrize(
        "filter_str, expect_error, expected_error_substring",
        [
            ("label = 2 AND text IS NOT NULL", False, None),
            ("some_col = 'my;string' AND another_col > 10", False, None),
            ("AND label = 2", True, "Error: Simulated parse error"),
            ("some_col =", True, "Error: Simulated parse error"),
            ("col = 'someval", True, "Error: Simulated parse error"),
            ("col = NULL", True, "Error: Simulated parse error"),
            (
                "col = 123; DROP TABLE foobar",
                True,
                "Invalid characters outside of string literals",
            ),
        ],
    )
    def test_filter_validation(
        self, mock_init_client, filter_str, expect_error, expected_error_substring
    ):
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client
        if expect_error:
            if "Invalid characters" not in expected_error_substring:
                mock_client.query.side_effect = Exception("Simulated parse error")
            with pytest.raises(ValueError) as exc_info:
                ClickHouseDatasource(
                    table="default.table_name",
                    dsn="clickhouse://user:password@localhost:8123/default",
                    filter=filter_str,
                )
            assert expected_error_substring in str(exc_info.value), (
                f"Expected substring '{expected_error_substring}' "
                f"not found in: {exc_info.value}"
            )
        else:
            mock_client.query.return_value = MagicMock()
            ds = ClickHouseDatasource(
                table="default.table_name",
                dsn="clickhouse://user:password@localhost:8123/default",
                filter=filter_str,
            )
            assert f"WHERE {filter_str}" in ds._query

    @pytest.mark.parametrize("parallelism", [1, 4])
    def test_get_read_tasks_with_filter(self, datasource, parallelism):
        datasource._filter = "label = 2 AND text IS NOT NULL"
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
        assert read_tasks[0].metadata.num_rows == 16

    def test_filter_none(self):
        table_name = "default.table_name"
        dsn = "clickhouse://user:password@localhost:8123/default"
        with mock.patch.object(ClickHouseDatasource, "_init_client") as mocked_init:
            mock_client = MagicMock()
            mocked_init.return_value = mock_client
            ds = ClickHouseDatasource(table=table_name, dsn=dsn, filter=None)
        assert "WHERE" not in ds._query
        assert ds._filter is None


@pytest.fixture(scope="session")
def ray_local_mode():
    ray.init(local_mode=True, num_cpus=1)
    yield
    ray.shutdown()


@pytest.fixture
def mock_clickhouse_sink_client():
    client = MagicMock()
    client.insert_arrow.return_value = QuerySummary({"written_rows": 3})
    return client


@pytest.fixture(autouse=True)
def patch_global_get_client(mock_clickhouse_sink_client):
    with patch(
        "clickhouse_connect.get_client", return_value=mock_clickhouse_sink_client
    ):
        yield


@pytest.mark.usefixtures("ray_local_mode")
class TestClickHouseDatasink:
    @pytest.fixture
    def datasink(self, mock_clickhouse_sink_client):
        sink = ClickHouseDatasink(
            table="default.test_table",
            dsn="clickhouse+http://user:pass@localhost:8123/default",
            mode=SinkMode.APPEND,
            table_settings=ClickHouseTableSettings(engine="MergeTree()"),
        )
        return sink

    @pytest.mark.parametrize(
        "mode",
        [
            SinkMode.OVERWRITE,
            SinkMode.APPEND,
            SinkMode.CREATE,
        ],
    )
    @pytest.mark.parametrize("table_exists", [True, False])
    def test_on_write_start_modes(
        self, datasink, mock_clickhouse_sink_client, mode, table_exists
    ):
        datasink._mode = mode
        if (mode in [SinkMode.OVERWRITE, SinkMode.CREATE]) or (
            mode == SinkMode.APPEND and not table_exists
        ):
            datasink._schema = pa.schema([("col1", pa.int32())])
        with patch.object(
            datasink, "_table_exists", return_value=table_exists
        ) as mock_tbl_exists, patch.object(
            datasink, "_get_existing_order_by", return_value="(prev_col)"
        ) as mock_get_order:
            if mode == SinkMode.CREATE and table_exists:
                with pytest.raises(ValueError, match="already exists.*CREATE"):
                    datasink.on_write_start()
                mock_tbl_exists.assert_called_once()
                mock_get_order.assert_not_called()
                mock_clickhouse_sink_client.command.assert_not_called()
            else:
                datasink.on_write_start()
                mock_tbl_exists.assert_called_once()
                if mode == SinkMode.OVERWRITE:
                    drop_cmd = "DROP TABLE IF EXISTS default.test_table"
                    mock_clickhouse_sink_client.command.assert_any_call(drop_cmd)
                    if table_exists:
                        mock_get_order.assert_called_once()
                    else:
                        mock_get_order.assert_not_called()

                elif mode == SinkMode.APPEND:
                    if table_exists:
                        mock_get_order.assert_called_once()
                    else:
                        mock_get_order.assert_not_called()
                        create_cmds = [
                            call_args[0][0]
                            for call_args in mock_clickhouse_sink_client.command.call_args_list
                            if "CREATE TABLE" in call_args[0][0]
                        ]
                        assert (
                            len(create_cmds) == 1
                        ), "Expected one CREATE TABLE for append + !exists."
                elif mode == SinkMode.CREATE:
                    if not table_exists:
                        mock_get_order.assert_not_called()
                        create_cmds = [
                            call_args[0][0]
                            for call_args in mock_clickhouse_sink_client.command.call_args_list
                            if "CREATE TABLE" in call_args[0][0]
                        ]
                        assert (
                            len(create_cmds) == 1
                        ), "Expected one CREATE TABLE for create + !exists."

    @pytest.mark.parametrize("mode", [SinkMode.OVERWRITE, SinkMode.APPEND])
    @pytest.mark.parametrize("table_exists", [True, False])
    @pytest.mark.parametrize("user_order_by", [None, "user_defined_col", "tuple()"])
    def test_write_behavior(
        self,
        datasink,
        mock_clickhouse_sink_client,
        mode,
        table_exists,
        user_order_by,
    ):
        datasink._mode = mode
        if user_order_by is not None:
            datasink._table_settings.order_by = user_order_by
        else:
            datasink._table_settings.order_by = None
        with patch.object(datasink, "_table_exists", return_value=table_exists), patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_sink_client
        ):
            if not table_exists or mode == SinkMode.OVERWRITE:
                datasink._schema = pa.schema([("col1", pa.int32())])
            datasink.on_write_start()
            rb = pa.record_batch([pa.array([1, 2, 3])], names=["col1"])
            block_data = pa.Table.from_batches([rb])
            ctx = TaskContext(1, "")
            results = datasink.write([block_data], ctx=ctx)
            assert results == [3]
            mock_clickhouse_sink_client.insert_arrow.assert_called()

    @pytest.mark.parametrize(
        "schema, expected_order_by",
        [
            (pa.schema([]), "tuple()"),
            (pa.schema([("ts", pa.timestamp("ns")), ("col2", pa.string())]), "ts"),
            (pa.schema([("col1", pa.string()), ("val", pa.int64())]), "val"),
            (pa.schema([("s1", pa.string()), ("s2", pa.large_string())]), "s1"),
        ],
    )
    def test_pick_best_arrow_field_for_order_by(
        self, datasink, mock_clickhouse_sink_client, schema, expected_order_by
    ):
        datasink._mode = SinkMode.OVERWRITE
        datasink._table_settings.order_by = None
        datasink._schema = schema
        with patch.object(datasink, "_table_exists", return_value=False), patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_sink_client
        ):
            datasink.on_write_start()
            # Build an empty table: 0 rows
            empty_table = pa.Table.from_batches([], schema=schema)
            datasink.write([empty_table], ctx=None)
            # Since we're skipping empty inserts now, we expect 0 calls:
            mock_clickhouse_sink_client.insert_arrow.assert_not_called()

    @pytest.mark.parametrize(
        "ddl_str, expected_order_by",
        [
            (
                "CREATE TABLE default.test_table (col1 Int32) ENGINE = MergeTree() ORDER BY col1",
                "col1",
            ),
            ("CREATE TABLE default.test_table (col1 Int32) ENGINE = MergeTree()", None),
            (
                "CREATE TABLE default.test_table (col1 Int32) ORDER BY city ENGINE = MergeTree()",
                "city",
            ),
            (
                "CREATE TABLE default.test_table (col1 Int32) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(date_col)",
                None,
            ),
        ],
    )
    def test_get_existing_order_by(
        self, datasink, mock_clickhouse_sink_client, ddl_str, expected_order_by
    ):
        mock_clickhouse_sink_client.command.return_value = ddl_str
        result = datasink._get_existing_order_by(mock_clickhouse_sink_client)
        assert result == expected_order_by

    @pytest.mark.parametrize(
        "table_settings, schema, expected_engine, expected_order_by_part, expected_clauses",
        [
            (
                ClickHouseTableSettings(),
                pa.schema([("col1", pa.int32())]),
                "MergeTree()",
                "ORDER BY col1",
                [],
            ),
            (
                ClickHouseTableSettings(engine="ReplacingMergeTree()"),
                pa.schema([("col1", pa.int32())]),
                "ReplacingMergeTree()",
                "ORDER BY col1",
                [],
            ),
            (
                ClickHouseTableSettings(order_by="user_col"),
                pa.schema([("col1", pa.int32())]),
                "MergeTree()",
                "ORDER BY user_col",
                [],
            ),
            (
                ClickHouseTableSettings(partition_by="toYYYYMMDD(ts)"),
                pa.schema([("ts", pa.timestamp("ns"))]),
                "MergeTree()",
                "ORDER BY ts",
                ["PARTITION BY toYYYYMMDD(ts)"],
            ),
            (
                ClickHouseTableSettings(primary_key="id"),
                pa.schema([("id", pa.int64()), ("val", pa.string())]),
                "MergeTree()",
                "ORDER BY id",
                ["PRIMARY KEY (id)"],
            ),
            (
                ClickHouseTableSettings(settings="index_granularity=8192"),
                pa.schema([("id", pa.int64())]),
                "MergeTree()",
                "ORDER BY id",
                ["SETTINGS index_granularity=8192"],
            ),
            (
                ClickHouseTableSettings(
                    engine="SummingMergeTree()",
                    order_by="col2",
                    partition_by="toYYYYMMDD(ts)",
                    primary_key="id",
                    settings="index_granularity=8192",
                ),
                pa.schema(
                    [
                        ("id", pa.int64()),
                        ("col2", pa.float64()),
                        ("ts", pa.timestamp("ns")),
                    ]
                ),
                "SummingMergeTree()",
                "ORDER BY col2",
                [
                    "PARTITION BY toYYYYMMDD(ts)",
                    "PRIMARY KEY (id)",
                    "SETTINGS index_granularity=8192",
                ],
            ),
        ],
    )
    def test_generate_create_table_sql(
        self,
        datasink,
        mock_clickhouse_sink_client,
        table_settings,
        schema,
        expected_engine,
        expected_order_by_part,
        expected_clauses,
    ):
        datasink._mode = SinkMode.OVERWRITE
        datasink._table_settings = table_settings
        datasink._schema = schema
        with patch.object(datasink, "_table_exists", return_value=False), patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_sink_client
        ):
            datasink.on_write_start()
            arrays = []
            for field in schema:
                if pa.types.is_integer(field.type):
                    arrays.append(pa.array([1, 2, 3], type=field.type))
                elif pa.types.is_floating(field.type):
                    arrays.append(pa.array([1.1, 2.2, 3.3], type=field.type))
                elif pa.types.is_timestamp(field.type):
                    arrays.append(pa.array([1, 2, 3], type=field.type))
                else:
                    arrays.append(pa.array(["a", "b", "c"], type=field.type))
            block_data = pa.Table.from_arrays(arrays, names=[f.name for f in schema])
            datasink.write([block_data], ctx=TaskContext(1, ""))
            create_sql = None
            for call_arg in mock_clickhouse_sink_client.command.call_args_list:
                sql_arg = call_arg[0][0]
                if "CREATE TABLE" in sql_arg:
                    create_sql = sql_arg
                    break
            assert create_sql is not None, "No CREATE TABLE statement was generated!"
            assert f"ENGINE = {expected_engine}" in create_sql
            assert expected_order_by_part in create_sql
            for clause in expected_clauses:
                assert clause in create_sql

    @pytest.mark.parametrize(
        "provided_schema,block_fields,expected_create_columns",
        [
            (
                pa.schema([("my_col", pa.float64()), ("ts", pa.timestamp("ns"))]),
                [("my_col", pa.int32()), ("ts", pa.int64())],
                ["`my_col` Float64", "`ts` DateTime64(3)"],
            ),
            (
                pa.schema([("my_col", pa.float64()), ("col2", pa.string())]),
                [("my_col", pa.int64()), ("col2", pa.large_string())],
                [
                    "`my_col` Float64",
                    "`col2` String",
                ],
            ),
            (
                pa.schema([("id", pa.int32()), ("val", pa.string())]),
                [("id", pa.int64()), ("val", pa.large_string())],
                [
                    "`id` Int32",
                    "`val` String",
                ],
            ),
            (
                pa.schema([("f1", pa.int32()), ("f2", pa.float64())]),
                [("f1", pa.int32()), ("f2", pa.int32())],
                [
                    "`f1` Int32",
                    "`f2` Float64",
                ],
            ),
        ],
    )
    def test_write_schema_override(
        self,
        datasink,
        mock_clickhouse_sink_client,
        provided_schema,
        block_fields,
        expected_create_columns,
    ):
        datasink._mode = SinkMode.CREATE
        datasink._table_settings.order_by = None
        with patch.object(datasink, "_table_exists", return_value=False), patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_sink_client
        ):
            datasink._schema = provided_schema
            datasink.on_write_start()
            arrays = []
            for name, typ in block_fields:
                if pa.types.is_integer(typ):
                    arrays.append(pa.array([1, 2, 3], type=typ))
                elif pa.types.is_string(typ) or pa.types.is_large_string(typ):
                    arrays.append(pa.array(["a", "b", "c"], type=typ))
                elif pa.types.is_timestamp(typ):
                    arrays.append(pa.array([1, 2, 3], type=typ))
                else:
                    arrays.append(pa.array([1.0, 2.0, 3.0], type=typ))
            block_data = pa.Table.from_arrays(
                arrays, names=[n for (n, _) in block_fields]
            )
            datasink.write([block_data], ctx=TaskContext(1, ""))
            create_sql = None
            for call_arg in mock_clickhouse_sink_client.command.call_args_list:
                sql_arg = call_arg[0][0]
                if "CREATE TABLE" in sql_arg:
                    create_sql = sql_arg
                    break
            assert create_sql is not None, "Expected CREATE TABLE to be issued."
            for expected_col_def in expected_create_columns:
                assert expected_col_def in create_sql

    @pytest.mark.parametrize(
        "max_insert_block_rows,block_sizes,expected_insert_calls",
        [
            (2, [6], [3]),
            (2, [6, 3], [3, 2]),
            (None, [6, 3], [1, 1]),
            (3, [3, 5, 2], [1, 2, 1]),
        ],
    )
    def test_chunked_inserts(
        self,
        datasink,
        mock_clickhouse_sink_client,
        max_insert_block_rows,
        block_sizes,
        expected_insert_calls,
    ):
        datasink._mode = SinkMode.CREATE
        datasink._schema = pa.schema([("col1", pa.int32())])
        datasink._max_insert_block_rows = max_insert_block_rows
        with patch.object(datasink, "_table_exists", return_value=False), patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_sink_client
        ):
            datasink.on_write_start()
            blocks = []
            for size in block_sizes:
                arr = pa.array(range(size), type=pa.int32())
                block_table = pa.Table.from_arrays([arr], names=["col1"])
                blocks.append(block_table)
            datasink.write(blocks, ctx=TaskContext(1, ""))
            insert_calls = [
                call_args[0][1]
                for call_args in mock_clickhouse_sink_client.insert_arrow.call_args_list
            ]
            actual_inserts = len(insert_calls)
            assert actual_inserts == sum(expected_insert_calls), (
                f"Expected total insert calls {sum(expected_insert_calls)}, "
                f"got {actual_inserts}."
            )
            offset = 0
            for block_idx, size in enumerate(block_sizes):
                calls_for_block = expected_insert_calls[block_idx]
                chunk_tables = insert_calls[offset : offset + calls_for_block]
                offset += calls_for_block
                total_rows = sum(tbl.num_rows for tbl in chunk_tables)
                assert total_rows == size, (
                    f"Block of size {size} was split incorrectly. "
                    f"Sum of chunk sizes is {total_rows}."
                )

    @pytest.mark.parametrize(
        "table_exists,mode,user_schema,block_fields,expected_error_regex",
        [
            (
                False,
                SinkMode.CREATE,
                pa.schema([("id", pa.int32())]),
                [("id", pa.int32()), ("extra_col", pa.int32())],
                r"(ArrowInvalid|Could not convert|field names are not matching.*)",
            ),
            (
                True,
                SinkMode.OVERWRITE,
                pa.schema([("id", pa.timestamp("ns"))]),
                [("id", pa.int32())],
                r"(ArrowInvalid|Could not convert|field names are not matching|Unsupported cast.*)",
            ),
        ],
    )
    def test_user_schema_block_mismatch(
        self,
        datasink,
        mock_clickhouse_sink_client,
        table_exists,
        mode,
        user_schema,
        block_fields,
        expected_error_regex,
    ):
        datasink._mode = mode
        datasink._schema = user_schema
        with patch.object(datasink, "_table_exists", return_value=table_exists), patch(
            "clickhouse_connect.get_client", return_value=mock_clickhouse_sink_client
        ):
            try:
                datasink.on_write_start()
            except ValueError:
                pass
            arrays = []
            for name, typ in block_fields:
                arrays.append(pa.array([1, 2, 3], type=typ))
            block_data = pa.Table.from_arrays(
                arrays, names=[n for (n, _) in block_fields]
            )
            with pytest.raises(
                (ValueError, pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError),
                match=expected_error_regex,
            ):
                datasink.write([block_data], ctx=TaskContext(1, ""))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
