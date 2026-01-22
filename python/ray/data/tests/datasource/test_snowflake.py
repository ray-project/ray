import base64
import os
import random
import string
from typing import Any, Dict, List, Tuple

import pytest
from snowflake.connector import connect

import ray
from ray.tests.conftest import *  # noqa

# Note: Snowflake secrets are only used in postmerge authenticated tests.


@pytest.fixture
def connection_parameters():
    private_key_b64 = os.getenv("SNOWFLAKE_PRIVATE_KEY")
    private_key_bytes = base64.b64decode(private_key_b64)
    parameters = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "private_key": private_key_bytes,
    }

    yield parameters


@pytest.fixture
def temp_table(connection_parameters):
    table_name = "".join([random.choice(string.ascii_uppercase) for _ in range(8)])

    yield table_name

    with connect(**connection_parameters) as connection, connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        connection.commit()


@pytest.mark.needs_credentials
def test_read(ray_start_regular_shared, connection_parameters):
    # This query fetches a small dataset with a variety of column types.
    query = "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER"

    # Read the data and check contents.
    dataset = ray.data.read_snowflake(query, connection_parameters)
    actual_column_names = dataset.schema().names
    actual_rows = [tuple(row.values()) for row in dataset.take_all()]
    expected_column_names, expected_rows = execute(query, connection_parameters)

    assert actual_column_names == expected_column_names
    assert sorted(actual_rows) == sorted(expected_rows)


@pytest.mark.needs_credentials
def test_write(ray_start_regular_shared, temp_table, connection_parameters):
    expected_column_names = ["title", "year", "score"]
    expected_rows = [
        ("Monty Python and the Holy Grail", 1975, 8.2),
        ("And Now for Something Completely Different", 1971, 7.5),
    ]

    # Create the table first
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {temp_table} (
        "title" VARCHAR(255),
        "year" INTEGER,
        "score" FLOAT
    )
    """
    execute(create_table_sql, connection_parameters)

    items = [dict(zip(expected_column_names, row)) for row in expected_rows]
    dataset = ray.data.from_items(items)

    dataset.write_snowflake(temp_table, connection_parameters)
    actual_column_names, actual_rows = execute(
        f"SELECT * FROM {temp_table}", connection_parameters
    )

    assert actual_column_names == expected_column_names
    assert sorted(actual_rows) == sorted(expected_rows)


@pytest.mark.needs_credentials
def execute(
    query: str, connection_parameters: Dict[str, str]
) -> Tuple[List[str], List[Tuple[Any]]]:
    """Execute a query on Snowflake and return the resulting data.

    Args:
        query: The SQL query to execute.
        connection_parameters: Connection params for snowflake.

    Returns:
        A two-tuple containing the column names and rows.
    """
    with connect(**connection_parameters) as connection, connection.cursor() as cursor:
        cursor.execute(query)
        column_names = [column_metadata.name for column_metadata in cursor.description]
        rows = cursor.fetchall()

    # TODO(mowen): Figure out how to actually handle the Decimal objects, we don't
    # want a divergenece in behavior here.
    # The Snowflake Python Connector represents numbers as `Decimal` objects.
    # rows = [
    #     tuple(float(value) if isinstance(value, Decimal) else value for value in row)
    #     for row in rows
    # ]

    return column_names, rows


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
