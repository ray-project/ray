import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.planner.plan_expression.expression_evaluator import (
    ExpressionEvaluator,
)


@pytest.fixture(scope="module")
def sample_data(tmpdir_factory):
    """Fixture to create and yield sample Parquet data, and clean up afterwards."""
    # Sample data for testing purposes
    data = {
        "age": [
            25,
            32,
            45,
            29,
            40,
            np.nan,
        ],  # List of ages, including a None value for testing
        "city": [
            "New York",
            "San Francisco",
            "Los Angeles",
            "Los Angeles",
            "San Francisco",
            "San Jose",
        ],
        "is_student": [False, True, False, False, True, None],  # Including a None value
    }

    # Create a PyArrow table from the sample data
    table = pa.table(data)

    # Use tmpdir_factory to create a temporary directory
    temp_dir = tmpdir_factory.mktemp("data")
    parquet_file = temp_dir.join("sample_data.parquet")

    # Write the table to a Parquet file in the temporary directory
    pq.write_table(table, str(parquet_file))

    # Yield the path to the Parquet file for testing
    yield str(parquet_file)


expressions_and_expected_data = [
    # Parameterized test cases with expressions and their expected results
    # Comparison Ops
    (
        "40 > age",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    (
        "40 >= age",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "30 < age",
        [
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "age >= 30",
        [
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "age == 40",
        [{"age": 40, "city": "San Francisco", "is_student": True}],
    ),
    (
        "is_student != True",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    (
        "age < 0",
        [],
    ),
    (
        "is_student == True",
        [
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "is_student == False",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    # Op 'in'
    (
        "city in ['Los Angeles', 'New York']",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    (
        "city in ['Los Angeles']",
        [
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    (
        "city in ['New York', 'San Francisco']",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "age in []",
        [],
    ),
    (
        "age in [25, 32, 45, 29, 40]",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "age in [25, 32, None]",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
        ],
    ),
    # Op 'not in'
    (
        "is_student not in [None]",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "is_student not in [True, None]",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    # Logical Ops 'and'
    (
        "age > 30 and is_student == True",
        [
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "city == 'Los Angeles' and age < 40",
        [
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    (
        "age < 40 and city in ['New York', 'Los Angeles']",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    # Logical Ops 'or'
    (
        "age < 30 or is_student == True",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "city == 'New York' or city == 'San Francisco'",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "age < 30 or age > 40",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    # Logical Ops combination 'and' and 'or'
    (
        "(age < 30 or age > 40) and is_student != True",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    # Op 'is_null'
    (
        "is_null(is_student)",
        [
            {"age": None, "city": "San Jose", "is_student": None},
        ],
    ),
    (
        "age < 40 or is_null(is_student)",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": None, "city": "San Jose", "is_student": None},
        ],
    ),
    # Op 'is_nan'
    (
        "is_nan(age)",
        [
            {"age": None, "city": "San Jose", "is_student": None},
        ],
    ),
    (
        "city in ['San Jose', 'Los Angeles'] and is_nan(age)",
        [
            {"age": None, "city": "San Jose", "is_student": None},
        ],
    ),
    # Op 'is_valid'
    (
        "is_valid(is_student)",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "is_valid(is_student) and is_valid(age)",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
]


@pytest.mark.parametrize("expression, expected_data", expressions_and_expected_data)
def test_filter(sample_data, expression, expected_data):
    """Test the filter functionality of the ExpressionEvaluator."""
    # Instantiate the ExpressionEvaluator with valid column names
    evaluator = ExpressionEvaluator()

    filters = evaluator.get_filters(expression)

    # Read the table from the Parquet file with the applied filters
    filtered_table = pq.read_table(sample_data, filters=filters)

    # Convert the filtered table back to a list of dictionaries for comparison
    result = filtered_table.to_pandas().to_dict(orient="records")

    def convert_nan_to_none(data):
        return [
            {k: (None if pd.isna(v) else v) for k, v in record.items()}
            for record in data
        ]

    # Convert NaN to None for comparison
    result_converted = convert_nan_to_none(result)

    assert result_converted == expected_data


def test_filter_bad_expression(sample_data):
    evaluator = ExpressionEvaluator()
    with pytest.raises(ValueError, match="Invalid syntax in the expression"):
        evaluator.get_filters("bad filter")

    filters = evaluator.get_filters("hi > 3")

    with pytest.raises(pa.ArrowInvalid):
        pq.read_table(sample_data, filters=filters)
