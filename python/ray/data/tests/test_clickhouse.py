import pytest
from ray.data._internal.datasource.clickhouse_datasource import (
    ClickHouseDatasource,
    format_selected_columns,
    parse_and_quote_table_identifier,
    quote_clickhouse_identifier,
)


def test_quote_single_identifier():
    assert quote_clickhouse_identifier("events") == "`events`"
    assert quote_clickhouse_identifier("my_table_1") == "`my_table_1`"
    assert quote_clickhouse_identifier("table`name") == "`table``name`"


def test_parse_and_quote_table_identifier():
    assert parse_and_quote_table_identifier("events") == "`events`"
    assert parse_and_quote_table_identifier("default.events") == "`default`.`events`"
    assert (
        parse_and_quote_table_identifier("my_db.user_events") == "`my_db`.`user_events`"
    )

    with pytest.raises(ValueError, match="Invalid table specification"):
        parse_and_quote_table_identifier("db.schema.table")

    with pytest.raises(ValueError, match="Invalid 'database.table' specification"):
        parse_and_quote_table_identifier("db.")


def test_format_selected_columns():
    assert format_selected_columns(None) == "*"
    assert format_selected_columns([]) == "*"
    assert format_selected_columns(["id", "created_at"]) == "`id`, `created_at`"

    with pytest.raises(ValueError, match="Invalid ClickHouse identifier"):
        format_selected_columns(["id", ""])


def test_clickhouse_datasource_query_generation():
    ds = ClickHouseDatasource(
        table="default.events",
        dsn="clickhouse+http://localhost:8123/default",
        columns=["id", "user_id"],
        order_by=(["created_at"], False),
    )
    assert (
        ds._query
        == "SELECT `id`, `user_id` FROM `default`.`events` ORDER BY `created_at`"
    )
