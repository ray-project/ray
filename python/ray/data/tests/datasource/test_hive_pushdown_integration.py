"""Hive table pushdown tests against a real HiveServer2."""

import pytest

import ray.data
from ray.data.expressions import col
from ray.data.tests.datasource.test_hive_integration import (
    _HOST,
    _connection_options,
    _temporary_table,
    _traced_factory,
)

pytestmark = [
    pytest.mark.skipif(not _HOST, reason="requires HiveServer2"),
    pytest.mark.usefixtures("ray_start_regular_shared"),
]


def test_hive_projection_limit_and_filter_order_over_real_hs2(tmp_path):
    options = _connection_options()
    values = "SELECT 1, 'a' UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c' UNION ALL SELECT 4, 'd' UNION ALL SELECT 5, 'e'"
    with _temporary_table(options, "id BIGINT, name STRING", values) as (table, _):
        trace = tmp_path / "statements"
        table_sql = f"`default`.`{table}`"

        def source(limit=None):
            trace.write_text("")
            return ray.data.read_hive(
                table, limit=limit, connection_factory=_traced_factory(options, trace)
            )

        rows = source(5).select_columns(["name"]).limit(2**31).limit(2).take_all()
        assert len(rows) == 2 and all(set(row) == {"name"} for row in rows)
        assert trace.read_text() == f"SELECT `name` FROM {table_sql} LIMIT 2\n"

        for source_limit in (True, False):
            dataset = source(5 if source_limit else None)
            if not source_limit:
                dataset = dataset.limit(5)
            rows = dataset.filter(expr=col("id") >= 3).limit(2).take_all()
            assert len(rows) == 2 and all(row["id"] >= 3 for row in rows)
            assert trace.read_text() == f"SELECT * FROM {table_sql} LIMIT 5\n"

        result = source().select_columns([]).materialize()
        assert result.count() == 5 and result.schema().names == []
        assert trace.read_text() == f"SELECT 1 AS `__ray_hive_row` FROM {table_sql}\n"
        assert source().limit(0).take_all() == []
        assert trace.read_text() == ""


def test_hive_projection_quoted_columns_over_real_hs2(tmp_path):
    options = _connection_options()
    with _temporary_table(
        options,
        "`a+b` BIGINT, ab BIGINT, `类别` BIGINT, `a``b` BIGINT",
        "SELECT 11, 22, 33, 44",
        storage_clause="STORED AS ORC",
    ) as (table, _):
        trace = tmp_path / "statements"
        for mode in ("column", "none"):
            trace.write_text("")
            columns = ["a+b", "类别", "a`b"]
            dataset = (
                ray.data.read_hive(
                    table, connection_factory=_traced_factory(options, trace, mode)
                )
                .select_columns(columns)
                .limit(1)
            )
            assert [dict(row) for row in dataset.take_all()] == [
                {"a+b": 11, "类别": 33, "a`b": 44}
            ]
            assert dataset.schema().names == columns
            assert trace.read_text() == f"SELECT * FROM `default`.`{table}` LIMIT 1\n"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
