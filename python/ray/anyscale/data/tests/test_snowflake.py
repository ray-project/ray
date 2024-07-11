import json
import random
import string
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import boto3
import pytest
from botocore.exceptions import ClientError
from snowflake.connector import connect

import ray


@pytest.fixture
def connection_parameters():
    secret_name = "runtime/ci/snowflake_connection_parameters"
    region_name = "us-east-2"

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        pytest.skip(f"Unable to locate Snowflake credentials: {e}")

    secret = json.loads(get_secret_value_response["SecretString"])
    parameters = {
        "user": secret["SNOWFLAKE_USER"],
        "account": secret["SNOWFLAKE_ACCOUNT"],
        "password": secret["SNOWFLAKE_PASSWORD"],
        "database": secret["SNOWFLAKE_DATABASE"],
        "schema": secret["SNOWFLAKE_SCHEMA"],
    }

    yield parameters


@pytest.fixture
def temp_table(connection_parameters):
    table_name = "".join([random.choice(string.ascii_uppercase) for _ in range(8)])

    yield table_name

    with connect(**connection_parameters) as connection, connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        connection.commit()


def test_read(connection_parameters):
    # This query fetches a small dataset with a variety of column types.
    query = "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER"

    dataset = ray.data.read_snowflake(query, connection_parameters)
    actual_column_names = dataset.schema().names
    actual_rows = [tuple(row.values()) for row in dataset.take_all()]

    expected_column_names, expected_rows = execute(query, connection_parameters)

    assert actual_column_names == expected_column_names
    assert sorted(actual_rows) == sorted(expected_rows)


def test_write(temp_table, connection_parameters):
    expected_column_names = ["title", "year", "score"]
    expected_rows = [
        ("Monty Python and the Holy Grail", 1975, 8.2),
        ("And Now for Something Completely Different", 1971, 7.5),
    ]
    items = [dict(zip(expected_column_names, row)) for row in expected_rows]
    dataset = ray.data.from_items(items)

    dataset.write_snowflake(temp_table, connection_parameters)
    actual_column_names, actual_rows = execute(
        f"SELECT * FROM {temp_table}", connection_parameters
    )

    assert actual_column_names == expected_column_names
    assert sorted(actual_rows) == sorted(expected_rows)


def execute(
    query: str, connection_parameters: Dict[str, str]
) -> Tuple[List[str], List[Tuple[Any]]]:
    """Execute a query on Snowflake and return the resulting data.

    Args:
        query: The SQL query to execute.

    Returns:
        A two-tuple containing the column names and rows.
    """
    with connect(**connection_parameters) as connection, connection.cursor() as cursor:
        cursor.execute(query)
        column_names = [column_metadata.name for column_metadata in cursor.description]
        rows = cursor.fetchall()

    # The Snowflake Python Connector represents numbers as `Decimal` objects.
    rows = [
        tuple(float(value) if isinstance(value, Decimal) else value for value in row)
        for row in rows
    ]

    return column_names, rows
