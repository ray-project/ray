import pytest
from ray.data._internal.datasource.clickhouse_datasource import (
    format_selected_columns,
    parse_and_quote_table_identifier,
    quote_clickhouse_identifier,
    validate_query_parameters,
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


def test_format_selected_columns():
    assert format_selected_columns(None) == "*"
    assert format_selected_columns([]) == "*"
    assert format_selected_columns(["id", "created_at"]) == "`id`, `created_at`"


def test_validate_query_parameters():
    # Valid typed parameter format {name:Type}
    filter_str = "tenant_id = {tenant:UInt32} AND event_type = {kind:String}"
    params = {"tenant": 42, "kind": "purchase"}
    validate_query_parameters(filter_str, params)

    # Missing parameters in dictionary
    with pytest.raises(ValueError, match="Missing values in `query_parameters`"):
        validate_query_parameters(filter_str, {"tenant": 42})

    # Non-typed parameter placeholder format %(tenant)s
    with pytest.raises(ValueError, match="Only strict typed placeholders"):
        validate_query_parameters("tenant_id = %(tenant)s", {"tenant": 42})

    # Mixed valid and invalid placeholders with the same name
    with pytest.raises(ValueError, match="Only strict typed placeholders"):
        validate_query_parameters(
            "tenant_id = {tenant:UInt32} AND other = {tenant}", {"tenant": 42}
        )

    # Query parameters provided without a filter
    with pytest.raises(ValueError, match="`query_parameters` provided but no `filter`"):
        validate_query_parameters(None, {"tenant": 42})
