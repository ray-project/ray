import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pkg_resources import parse_version

import ray
from ray.data.expressions import col
from ray.data.tests.conftest import get_pyarrow_version
from ray.tests.conftest import *  # noqa


def test_filter_mutex(ray_start_regular_shared, tmp_path):
    """Test filter op."""

    # Generate sample data
    data = {
        "sepal.length": [4.8, 5.1, 5.7, 6.3, 7.0],
        "sepal.width": [3.0, 3.3, 3.5, 3.2, 2.8],
        "petal.length": [1.4, 1.7, 4.2, 5.4, 6.1],
        "petal.width": [0.2, 0.4, 1.5, 2.1, 2.4],
    }
    df = pd.DataFrame(data)

    # Define the path for the Parquet file in the tmp_path directory
    parquet_file = tmp_path / "sample_data.parquet"

    # Write DataFrame to a Parquet file
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)

    # Load parquet dataset
    parquet_ds = ray.data.read_parquet(str(parquet_file))

    # Filter using lambda (UDF)
    with pytest.raises(
        ValueError,
    ):
        parquet_ds.filter(
            fn=lambda r: r["sepal.length"] > 5.0, expr="sepal.length > 5.0"
        )

    with pytest.raises(ValueError, match="must be a UserDefinedFunction"):
        parquet_ds.filter(fn="sepal.length > 5.0")


def test_filter_with_expressions(ray_start_regular_shared, tmp_path):
    """Test filtering with expressions."""

    # Generate sample data
    data = {
        "sepal.length": [4.8, 5.1, 5.7, 6.3, 7.0],
        "sepal.width": [3.0, 3.3, 3.5, 3.2, 2.8],
        "petal.length": [1.4, 1.7, 4.2, 5.4, 6.1],
        "petal.width": [0.2, 0.4, 1.5, 2.1, 2.4],
    }
    df = pd.DataFrame(data)

    # Define the path for the Parquet file in the tmp_path directory
    parquet_file = tmp_path / "sample_data.parquet"

    # Write DataFrame to a Parquet file
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)

    # Load parquet dataset
    parquet_ds = ray.data.read_parquet(str(parquet_file))

    # Filter using lambda (UDF)
    filtered_udf_ds = parquet_ds.filter(lambda r: r["sepal.length"] > 5.0)
    filtered_udf_data = filtered_udf_ds.to_pandas()

    # Filter using expressions
    filtered_expr_ds = parquet_ds.filter(expr="sepal.length > 5.0")
    filtered_expr_data = filtered_expr_ds.to_pandas()

    # Assert the filtered data is the same
    assert set(filtered_udf_data["sepal.length"]) == set(
        filtered_expr_data["sepal.length"]
    )
    assert len(filtered_udf_data) == len(filtered_expr_data)

    # Verify correctness of filtered results: only rows with 'sepal.length' > 5.0
    assert all(
        filtered_expr_data["sepal.length"] > 5.0
    ), "Filtered data contains rows with 'sepal.length' <= 5.0"
    assert all(
        filtered_udf_data["sepal.length"] > 5.0
    ), "UDF-filtered data contains rows with 'sepal.length' <= 5.0"


def test_filter_with_invalid_expression(ray_start_regular_shared, tmp_path):
    """Test filtering with invalid expressions."""

    # Generate sample data
    data = {
        "sepal.length": [4.8, 5.1, 5.7, 6.3, 7.0],
        "sepal.width": [3.0, 3.3, 3.5, 3.2, 2.8],
        "petal.length": [1.4, 1.7, 4.2, 5.4, 6.1],
        "petal.width": [0.2, 0.4, 1.5, 2.1, 2.4],
    }
    df = pd.DataFrame(data)

    # Define the path for the Parquet file in the tmp_path directory
    parquet_file = tmp_path / "sample_data.parquet"

    # Write DataFrame to a Parquet file
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)

    # Load parquet dataset
    parquet_ds = ray.data.read_parquet(str(parquet_file))

    with pytest.raises(ValueError, match="Invalid syntax in the expression"):
        parquet_ds.filter(expr="fake_news super fake")

    fake_column_ds = parquet_ds.filter(expr="sepal_length_123 > 1")
    # With predicate pushdown, the error is raised during file reading
    # and wrapped in RayTaskError
    with pytest.raises(
        (ray.exceptions.RayTaskError, RuntimeError), match="sepal_length_123"
    ):
        fake_column_ds.to_pandas()


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="predicate expressions require PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "data_source",
    [
        pytest.param("from_items", id="arrow_blocks"),
        pytest.param("from_pandas", id="pandas_blocks"),
    ],
)
@pytest.mark.parametrize(
    "predicate_expr, test_data, expected_indices, test_description",
    [
        # Simple comparison filters
        pytest.param(
            col("age") >= 21,
            [
                {"age": 20, "name": "Alice"},
                {"age": 21, "name": "Bob"},
                {"age": 25, "name": "Charlie"},
                {"age": 30, "name": "David"},
            ],
            [1, 2, 3],  # Indices of rows that should remain
            "age_greater_equal_filter",
        ),
        pytest.param(
            col("score") > 50,
            [
                {"score": 30, "status": "fail"},
                {"score": 50, "status": "borderline"},
                {"score": 70, "status": "pass"},
                {"score": 90, "status": "excellent"},
            ],
            [2, 3],
            "score_greater_than_filter",
        ),
        pytest.param(
            col("category") == "premium",
            [
                {"category": "basic", "price": 10},
                {"category": "premium", "price": 50},
                {"category": "standard", "price": 25},
                {"category": "premium", "price": 75},
            ],
            [1, 3],
            "equality_string_filter",
        ),
        # Complex logical filters
        pytest.param(
            (col("age") >= 18) & (col("active")),
            [
                {"age": 17, "active": True},
                {"age": 18, "active": False},
                {"age": 25, "active": True},
                {"age": 30, "active": True},
            ],
            [2, 3],
            "logical_and_filter",
        ),
        pytest.param(
            (col("status") == "approved") | (col("priority") == "high"),
            [
                {"status": "pending", "priority": "low"},
                {"status": "approved", "priority": "low"},
                {"status": "pending", "priority": "high"},
                {"status": "rejected", "priority": "high"},
            ],
            [1, 2, 3],
            "logical_or_filter",
        ),
        # Null handling filters
        pytest.param(
            col("value").is_not_null(),
            [
                {"value": None, "id": 1},
                {"value": 0, "id": 2},
                {"value": None, "id": 3},
                {"value": 42, "id": 4},
            ],
            [1, 3],
            "not_null_filter",
        ),
        pytest.param(
            col("name").is_null(),
            [
                {"name": "Alice", "id": 1},
                {"name": None, "id": 2},
                {"name": "Bob", "id": 3},
                {"name": None, "id": 4},
            ],
            [1, 3],
            "is_null_filter",
        ),
        # Complex multi-condition filters
        pytest.param(
            col("value").is_not_null() & (col("value") > 0),
            [
                {"value": None, "type": "missing"},
                {"value": -5, "type": "negative"},
                {"value": 0, "type": "zero"},
                {"value": 10, "type": "positive"},
            ],
            [3],
            "null_aware_positive_filter",
        ),
        # String operations
        pytest.param(
            col("name").is_not_null() & (col("name") != "excluded"),
            [
                {"name": "included", "id": 1},
                {"name": "excluded", "id": 2},
                {"name": None, "id": 3},
                {"name": "allowed", "id": 4},
            ],
            [0, 3],
            "string_exclusion_filter",
        ),
        # Additional comparison operations
        pytest.param(
            col("age") > 25,
            [
                {"age": 20, "name": "Alice"},
                {"age": 25, "name": "Bob"},
                {"age": 30, "name": "Charlie"},
                {"age": 35, "name": "David"},
            ],
            [2, 3],
            "greater_than_filter",
        ),
        pytest.param(
            col("age") < 25,
            [
                {"age": 20, "name": "Alice"},
                {"age": 25, "name": "Bob"},
                {"age": 30, "name": "Charlie"},
            ],
            [0],
            "less_than_filter",
        ),
        pytest.param(
            col("age") <= 25,
            [
                {"age": 20, "name": "Alice"},
                {"age": 25, "name": "Bob"},
                {"age": 30, "name": "Charlie"},
            ],
            [0, 1],
            "less_than_equal_filter",
        ),
        # Membership operations
        pytest.param(
            col("category").is_in(["A", "B"]),
            [
                {"category": "A", "value": 1},
                {"category": "B", "value": 2},
                {"category": "C", "value": 3},
                {"category": "D", "value": 4},
                {"category": "A", "value": 5},
            ],
            [0, 1, 4],
            "is_in_filter",
        ),
        pytest.param(
            col("category").not_in(["A", "B"]),
            [
                {"category": "A", "value": 1},
                {"category": "B", "value": 2},
                {"category": "C", "value": 3},
                {"category": "D", "value": 4},
            ],
            [2, 3],  # These are indices not the actual values
            "not_in_filter",
        ),
        # Negation operations
        pytest.param(
            ~(col("category") == "reject"),
            [
                {"category": "accept", "id": 1},
                {"category": "reject", "id": 2},
                {"category": "pending", "id": 3},
                {"category": "reject", "id": 4},
            ],
            [0, 2],
            "negation_filter",
        ),
        # Nested complex expressions
        pytest.param(
            (col("score") >= 50) & (col("grade") != "F") & col("active"),
            [
                {"score": 45, "grade": "F", "active": True},
                {"score": 55, "grade": "D", "active": True},
                {"score": 75, "grade": "B", "active": False},
                {"score": 85, "grade": "A", "active": True},
            ],
            [1, 3],
            "complex_nested_filter",
        ),
    ],
)
def test_filter_with_predicate_expressions(
    ray_start_regular_shared,
    data_source,
    predicate_expr,
    test_data,
    expected_indices,
    test_description,
):
    """Test filter() with Ray Data predicate expressions on both Arrow and pandas blocks."""
    # Create dataset based on data_source parameter
    if data_source == "from_items":
        ds = ray.data.from_items(test_data)
    else:  # from_pandas
        ds = ray.data.from_pandas([pd.DataFrame(test_data)])

    # Apply filter with predicate expression
    filtered_ds = ds.filter(expr=predicate_expr)

    # Convert to list and verify results
    result_data = filtered_ds.to_pandas().to_dict("records")
    expected_data = [test_data[i] for i in expected_indices]

    # Use pandas testing for consistent comparison
    result_df = pd.DataFrame(result_data)
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True),
        expected_df.reset_index(drop=True),
        check_dtype=False,
    )


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="predicate expressions require PyArrow >= 20.0.0",
)
def test_filter_predicate_expr_vs_function_consistency(ray_start_regular_shared):
    """Test that predicate expressions produce the same results as equivalent functions."""
    test_data = [
        {"age": 20, "score": 85, "active": True},
        {"age": 25, "score": 45, "active": False},
        {"age": 30, "score": 95, "active": True},
        {"age": 18, "score": 60, "active": True},
    ]

    ds = ray.data.from_items(test_data)

    # Test simple comparison
    predicate_result = ds.filter(expr=col("age") >= 21).to_pandas()
    function_result = ds.filter(fn=lambda row: row["age"] >= 21).to_pandas()
    pd.testing.assert_frame_equal(predicate_result, function_result, check_dtype=False)

    # Test complex logical expression
    complex_predicate = (col("age") >= 21) & (col("score") > 80) & col("active")
    predicate_result = ds.filter(expr=complex_predicate).to_pandas()
    function_result = ds.filter(
        fn=lambda row: row["age"] >= 21 and row["score"] > 80 and row["active"]
    ).to_pandas()
    pd.testing.assert_frame_equal(predicate_result, function_result, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="predicate expressions require PyArrow >= 20.0.0",
)
def test_filter_predicate_with_different_block_formats(ray_start_regular_shared):
    """Test that predicate expressions work with different block formats (pandas/arrow)."""
    test_data = [
        {"category": "A", "value": 10},
        {"category": "B", "value": 20},
        {"category": "A", "value": 30},
        {"category": "C", "value": 40},
    ]

    # Test with different data sources that produce different block formats

    # From items (typically arrow)
    ds_items = ray.data.from_items(test_data)
    result_items = ds_items.filter(expr=col("category") == "A").to_pandas()

    # From pandas (pandas blocks)
    df = pd.DataFrame(test_data)
    ds_pandas = ray.data.from_pandas([df])
    result_pandas = ds_pandas.filter(expr=col("category") == "A").to_pandas()

    # Results should be identical (reset indices for comparison)
    expected_df = pd.DataFrame(
        [
            {"category": "A", "value": 10},
            {"category": "A", "value": 30},
        ]
    )

    pd.testing.assert_frame_equal(
        result_items.reset_index(drop=True),
        expected_df.reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        result_pandas.reset_index(drop=True),
        expected_df.reset_index(drop=True),
        check_dtype=False,
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
