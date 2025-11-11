import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pkg_resources import parse_version

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data.datatype import DataType
from ray.data.exceptions import UserCodeException
from ray.data.expressions import col, lit, udf
from ray.data.tests.conftest import *  # noqa
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "column_name, expr, expected_value",
    [
        # Arithmetic operations
        ("result", col("id") + 1, 1),  # 0 + 1 = 1
        ("result", col("id") + 5, 5),  # 0 + 5 = 5
        ("result", col("id") - 1, -1),  # 0 - 1 = -1
        ("result", col("id") * 2, 0),  # 0 * 2 = 0
        ("result", col("id") * 3, 0),  # 0 * 3 = 0
        ("result", col("id") / 2, 0.0),  # 0 / 2 = 0.0
        # More complex arithmetic
        ("result", (col("id") + 1) * 2, 2),  # (0 + 1) * 2 = 2
        ("result", (col("id") * 2) + 3, 3),  # 0 * 2 + 3 = 3
        # Comparison operations
        ("result", col("id") > 0, False),  # 0 > 0 = False
        ("result", col("id") >= 0, True),  # 0 >= 0 = True
        ("result", col("id") < 1, True),  # 0 < 1 = True
        ("result", col("id") <= 0, True),  # 0 <= 0 = True
        ("result", col("id") == 0, True),  # 0 == 0 = True
        # Operations with literals
        ("result", col("id") + lit(10), 10),  # 0 + 10 = 10
        ("result", col("id") * lit(5), 0),  # 0 * 5 = 0
        ("result", lit(2) + col("id"), 2),  # 2 + 0 = 2
        ("result", lit(10) / (col("id") + 1), 10.0),  # 10 / (0 + 1) = 10.0
    ],
)
def test_with_column(
    ray_start_regular_shared,
    column_name,
    expr,
    expected_value,
    target_max_block_size_infinite_or_default,
):
    """Verify that `with_column` works with various operations."""
    ds = ray.data.range(5).with_column(column_name, expr)
    result = ds.take(1)[0]
    assert result["id"] == 0
    assert result[column_name] == expected_value


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_nonexistent_column(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Verify that referencing a non-existent column with col() raises an exception."""
    # Create a dataset with known column "id"
    ds = ray.data.range(5)

    # Try to reference a non-existent column - this should raise an exception
    with pytest.raises(UserCodeException):
        ds.with_column("result", col("nonexistent_column") + 1).materialize()


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_multiple_expressions(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Verify that `with_column` correctly handles multiple expressions at once."""
    ds = ray.data.range(5)

    ds = ds.with_column("plus_one", col("id") + 1)
    ds = ds.with_column("times_two", col("id") * 2)
    ds = ds.with_column("ten_minus_id", 10 - col("id"))

    first_row = ds.take(1)[0]
    assert first_row["id"] == 0
    assert first_row["plus_one"] == 1
    assert first_row["times_two"] == 0
    assert first_row["ten_minus_id"] == 10

    # Ensure all new columns exist in the schema.
    assert set(ds.schema().names) == {"id", "plus_one", "times_two", "ten_minus_id"}


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "udf_function, column_name, expected_result",
    [
        # Single column UDF - add one to each value
        pytest.param(
            lambda: udf(DataType.int64())(lambda x: pc.add(x, 1)),
            "add_one",
            1,  # 0 + 1 = 1
            id="single_column_add_one",
        ),
        # Single column UDF - multiply by 2
        pytest.param(
            lambda: udf(DataType.int64())(lambda x: pc.multiply(x, 2)),
            "times_two",
            0,  # 0 * 2 = 0
            id="single_column_multiply",
        ),
        # Single column UDF - square the value
        pytest.param(
            lambda: udf(DataType.int64())(lambda x: pc.multiply(x, x)),
            "squared",
            0,  # 0 * 0 = 0
            id="single_column_square",
        ),
        # Single column UDF with string return type
        pytest.param(
            lambda: udf(DataType.string())(lambda x: pc.cast(x, pa.string())),
            "id_str",
            "0",  # Convert 0 to "0"
            id="single_column_to_string",
        ),
        # Single column UDF with float return type
        pytest.param(
            lambda: udf(DataType.float64())(lambda x: pc.divide(x, 2.0)),
            "half",
            0.0,  # 0 / 2.0 = 0.0
            id="single_column_divide_float",
        ),
    ],
)
def test_with_column_udf_single_column(
    ray_start_regular_shared,
    udf_function,
    column_name,
    expected_result,
    target_max_block_size_infinite_or_default,
):
    """Test UDFExpr functionality with single column operations in with_column."""
    ds = ray.data.range(5)
    udf_fn = udf_function()

    # Apply the UDF to the "id" column
    ds_with_udf = ds.with_column(column_name, udf_fn(col("id")))

    result = ds_with_udf.take(1)[0]
    assert result["id"] == 0
    assert result[column_name] == expected_result


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "test_scenario",
    [
        # Multi-column UDF - add two columns
        pytest.param(
            {
                "data": [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
                "udf": lambda: udf(DataType.int64())(lambda x, y: pc.add(x, y)),
                "column_name": "sum_ab",
                "expected_first": 3,  # 1 + 2 = 3
                "expected_second": 7,  # 3 + 4 = 7
            },
            id="multi_column_add",
        ),
        # Multi-column UDF - multiply two columns
        pytest.param(
            {
                "data": [{"x": 2, "y": 3}, {"x": 4, "y": 5}],
                "udf": lambda: udf(DataType.int64())(lambda x, y: pc.multiply(x, y)),
                "column_name": "product_xy",
                "expected_first": 6,  # 2 * 3 = 6
                "expected_second": 20,  # 4 * 5 = 20
            },
            id="multi_column_multiply",
        ),
        # Multi-column UDF - string concatenation
        pytest.param(
            {
                "data": [
                    {"first": "John", "last": "Doe"},
                    {"first": "Jane", "last": "Smith"},
                ],
                "udf": lambda: udf(DataType.string())(
                    lambda first, last: pc.binary_join_element_wise(first, last, " ")
                ),
                "column_name": "full_name",
                "expected_first": "John Doe",
                "expected_second": "Jane Smith",
            },
            id="multi_column_string_concat",
        ),
    ],
)
def test_with_column_udf_multi_column(
    ray_start_regular_shared,
    test_scenario,
    target_max_block_size_infinite_or_default,
):
    """Test UDFExpr functionality with multi-column operations in with_column."""
    data = test_scenario["data"]
    udf_fn = test_scenario["udf"]()
    column_name = test_scenario["column_name"]
    expected_first = test_scenario["expected_first"]
    expected_second = test_scenario["expected_second"]

    ds = ray.data.from_items(data)

    # Apply UDF to multiple columns based on the scenario
    if "a" in data[0] and "b" in data[0]:
        ds_with_udf = ds.with_column(column_name, udf_fn(col("a"), col("b")))
    elif "x" in data[0] and "y" in data[0]:
        ds_with_udf = ds.with_column(column_name, udf_fn(col("x"), col("y")))
    else:  # first/last name scenario
        ds_with_udf = ds.with_column(column_name, udf_fn(col("first"), col("last")))

    results = ds_with_udf.take(2)
    assert results[0][column_name] == expected_first
    assert results[1][column_name] == expected_second


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "expression_scenario",
    [
        # UDF in arithmetic expression
        pytest.param(
            {
                "expression_factory": lambda add_one_udf: add_one_udf(col("id")) * 2,
                "expected": 2,  # (0 + 1) * 2 = 2
                "column_name": "udf_times_two",
            },
            id="udf_in_arithmetic",
        ),
        # UDF with literal addition
        pytest.param(
            {
                "expression_factory": lambda add_one_udf: add_one_udf(col("id"))
                + lit(10),
                "expected": 11,  # (0 + 1) + 10 = 11
                "column_name": "udf_plus_literal",
            },
            id="udf_plus_literal",
        ),
        # UDF in comparison
        pytest.param(
            {
                "expression_factory": lambda add_one_udf: add_one_udf(col("id")) > 0,
                "expected": True,  # (0 + 1) > 0 = True
                "column_name": "udf_comparison",
            },
            id="udf_in_comparison",
        ),
        # Nested UDF operations (UDF + regular expression)
        pytest.param(
            {
                "expression_factory": lambda add_one_udf: add_one_udf(col("id") + 5),
                "expected": 6,  # add_one(0 + 5) = add_one(5) = 6
                "column_name": "nested_udf",
            },
            id="nested_udf_expression",
        ),
    ],
)
def test_with_column_udf_in_complex_expressions(
    ray_start_regular_shared,
    expression_scenario,
    target_max_block_size_infinite_or_default,
):
    """Test UDFExpr functionality in complex expressions with with_column."""
    ds = ray.data.range(5)

    # Create a simple add_one UDF for use in expressions
    @udf(DataType.int64())
    def add_one(x: pa.Array) -> pa.Array:
        return pc.add(x, 1)

    expression = expression_scenario["expression_factory"](add_one)
    expected = expression_scenario["expected"]
    column_name = expression_scenario["column_name"]

    ds_with_expr = ds.with_column(column_name, expression)

    result = ds_with_expr.take(1)[0]
    assert result["id"] == 0
    assert result[column_name] == expected


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_udf_multiple_udfs(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test applying multiple UDFs in sequence with with_column."""
    ds = ray.data.range(5)

    # Define multiple UDFs
    @udf(DataType.int64())
    def add_one(x: pa.Array) -> pa.Array:
        return pc.add(x, 1)

    @udf(DataType.int64())
    def multiply_by_two(x: pa.Array) -> pa.Array:
        return pc.multiply(x, 2)

    @udf(DataType.float64())
    def divide_by_three(x: pa.Array) -> pa.Array:
        return pc.divide(x, 3.0)

    # Apply UDFs in sequence
    ds = ds.with_column("plus_one", add_one(col("id")))
    ds = ds.with_column("times_two", multiply_by_two(col("plus_one")))
    ds = ds.with_column("div_three", divide_by_three(col("times_two")))

    # Convert to pandas and compare with expected result
    result_df = ds.to_pandas()

    expected_df = pd.DataFrame(
        {
            "id": [0, 1, 2, 3, 4],
            "plus_one": [1, 2, 3, 4, 5],  # id + 1
            "times_two": [2, 4, 6, 8, 10],  # (id + 1) * 2
            "div_three": [
                2.0 / 3.0,
                4.0 / 3.0,
                2.0,
                8.0 / 3.0,
                10.0 / 3.0,
            ],  # ((id + 1) * 2) / 3
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_mixed_udf_and_regular_expressions(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test mixing UDF expressions and regular expressions in with_column operations."""
    ds = ray.data.range(5)

    # Define a UDF for testing
    @udf(DataType.int64())
    def multiply_by_three(x: pa.Array) -> pa.Array:
        return pc.multiply(x, 3)

    # Mix regular expressions and UDF expressions
    ds = ds.with_column("plus_ten", col("id") + 10)  # Regular expression
    ds = ds.with_column("times_three", multiply_by_three(col("id")))  # UDF expression
    ds = ds.with_column("minus_five", col("id") - 5)  # Regular expression
    ds = ds.with_column(
        "udf_plus_regular", multiply_by_three(col("id")) + col("plus_ten")
    )  # Mixed: UDF + regular
    ds = ds.with_column(
        "comparison", col("times_three") > col("plus_ten")
    )  # Regular expression using UDF result

    # Convert to pandas and compare with expected result
    result_df = ds.to_pandas()

    expected_df = pd.DataFrame(
        {
            "id": [0, 1, 2, 3, 4],
            "plus_ten": [10, 11, 12, 13, 14],  # id + 10
            "times_three": [0, 3, 6, 9, 12],  # id * 3
            "minus_five": [-5, -4, -3, -2, -1],  # id - 5
            "udf_plus_regular": [10, 14, 18, 22, 26],  # (id * 3) + (id + 10)
            "comparison": [False, False, False, False, False],  # times_three > plus_ten
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_udf_invalid_return_type_validation(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test that UDFs returning invalid types raise TypeError with clear message."""
    ds = ray.data.range(3)

    # Test UDF returning invalid type (dict) - expecting string but returning dict
    @udf(DataType.string())
    def invalid_dict_return(x: pa.Array) -> dict:
        return {"invalid": "return_type"}

    # Test UDF returning invalid type (str) - expecting string but returning plain str
    @udf(DataType.string())
    def invalid_str_return(x: pa.Array) -> str:
        return "invalid_string"

    # Test UDF returning invalid type (int) - expecting int64 but returning plain int
    @udf(DataType.int64())
    def invalid_int_return(x: pa.Array) -> int:
        return 42

    # Test each invalid return type
    test_cases = [
        (invalid_dict_return, "dict"),
        (invalid_str_return, "str"),
        (invalid_int_return, "int"),
    ]

    for invalid_udf, expected_type_name in test_cases:
        with pytest.raises((RayTaskError, UserCodeException)) as exc_info:
            ds.with_column("invalid_col", invalid_udf(col("id"))).take(1)

        # The actual TypeError gets wrapped, so we need to check the exception chain
        error_message = str(exc_info.value)
        assert f"returned invalid type {expected_type_name}" in error_message
        assert "Expected type" in error_message
        assert "pandas.Series" in error_message and "numpy.ndarray" in error_message


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "scenario",
    [
        pytest.param(
            {
                "data": [
                    {"name": "Alice"},
                    {"name": "Bob"},
                    {"name": "Charlie"},
                ],
                "expr_factory": lambda: col("name") + "_X",
                "column_name": "name_with_suffix",
                "expected": ["Alice_X", "Bob_X", "Charlie_X"],
            },
            id="string_col_plus_python_literal_rhs",
        ),
        pytest.param(
            {
                "data": [
                    {"name": "Alice"},
                    {"name": "Bob"},
                    {"name": "Charlie"},
                ],
                "expr_factory": lambda: "_X" + col("name"),
                "column_name": "name_with_prefix",
                "expected": ["_XAlice", "_XBob", "_XCharlie"],
            },
            id="python_literal_lhs_plus_string_col",
        ),
        pytest.param(
            {
                "data": [
                    {"first": "John", "last": "Doe"},
                    {"first": "Jane", "last": "Smith"},
                ],
                "expr_factory": lambda: col("first") + col("last"),
                "column_name": "full_name",
                "expected": ["JohnDoe", "JaneSmith"],
            },
            id="string_col_plus_string_col",
        ),
        pytest.param(
            {
                "arrow_table": pa.table(
                    {"name": pa.array(["Alice", "Bob"]).dictionary_encode()}
                ),
                "expr_factory": lambda: col("name") + "_X",
                "column_name": "name_with_suffix",
                "expected": ["Alice_X", "Bob_X"],
            },
            id="dict_encoded_string_col_plus_literal_rhs",
        ),
        pytest.param(
            {
                "data": [
                    {"name": "Alice"},
                    {"name": "Bob"},
                ],
                "expr_factory": lambda: col("name") + lit("_X"),
                "column_name": "name_with_suffix",
                "expected": ["Alice_X", "Bob_X"],
            },
            id="string_col_plus_lit_literal_rhs",
        ),
    ],
)
def test_with_column_string_concat_combinations(
    ray_start_regular_shared,
    scenario,
):
    if "arrow_table" in scenario:
        ds = ray.data.from_arrow(scenario["arrow_table"])
    else:
        ds = ray.data.from_items(scenario["data"])

    expr = scenario["expr_factory"]()
    column_name = scenario["column_name"]

    ds2 = ds.with_column(column_name, expr)
    out = ds2.to_pandas()
    assert out[column_name].tolist() == scenario["expected"]


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_string_concat_type_mismatch_raises(
    ray_start_regular_shared,
):
    # int + string should raise a user-facing error
    ds = ray.data.range(3)
    with pytest.raises((RayTaskError, UserCodeException)):
        ds.with_column("bad", col("id") + "_X").materialize()


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "expression, expected_column_data, test_description",
    [
        # Floor division operations
        pytest.param(
            col("id") // 2,
            [0, 0, 1, 1, 2],  # [0//2, 1//2, 2//2, 3//2, 4//2]
            "floor_division_by_literal",
        ),
        pytest.param(
            lit(10) // (col("id") + 2),
            [5, 3, 2, 2, 1],  # [10//(0+2), 10//(1+2), 10//(2+2), 10//(3+2), 10//(4+2)]
            "literal_floor_division_by_expression",
        ),
        # Not equal operations
        pytest.param(
            col("id") != 2,
            [True, True, False, True, True],  # [0!=2, 1!=2, 2!=2, 3!=2, 4!=2]
            "not_equal_operation",
        ),
        # Null checking operations
        pytest.param(
            col("id").is_null(),
            [False, False, False, False, False],  # None of the values are null
            "is_null_operation",
        ),
        pytest.param(
            col("id").is_not_null(),
            [True, True, True, True, True],  # All values are not null
            "is_not_null_operation",
        ),
        # Logical NOT operations
        pytest.param(
            ~(col("id") == 2),
            [True, True, False, True, True],  # ~[0==2, 1==2, 2==2, 3==2, 4==2]
            "logical_not_operation",
        ),
    ],
)
def test_with_column_floor_division_and_logical_operations(
    ray_start_regular_shared,
    expression,
    expected_column_data,
    test_description,
):
    """Test floor division, not equal, null checks, and logical NOT operations with with_column."""
    ds = ray.data.range(5)
    result_ds = ds.with_column("result", expression)

    # Convert to pandas and assert on the whole dataframe
    result_df = result_ds.to_pandas()
    expected_df = pd.DataFrame({"id": [0, 1, 2, 3, 4], "result": expected_column_data})

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "test_data, expression, expected_results, test_description",
    [
        # Test with null values
        pytest.param(
            [{"value": 1}, {"value": None}, {"value": 3}],
            col("value").is_null(),
            [False, True, False],
            "is_null_with_actual_nulls",
        ),
        pytest.param(
            [{"value": 1}, {"value": None}, {"value": 3}],
            col("value").is_not_null(),
            [True, False, True],
            "is_not_null_with_actual_nulls",
        ),
        # Test is_in operations
        pytest.param(
            [{"value": 1}, {"value": 2}, {"value": 3}],
            col("value").is_in([1, 3]),
            [True, False, True],
            "isin_operation",
        ),
        pytest.param(
            [{"value": 1}, {"value": 2}, {"value": 3}],
            col("value").not_in([1, 3]),
            [False, True, False],
            "not_in_operation",
        ),
        # Test string operations
        pytest.param(
            [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}],
            col("name") == "Bob",
            [False, True, False],
            "string_equality",
        ),
        pytest.param(
            [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}],
            col("name") != "Bob",
            [True, False, True],
            "string_not_equal",
        ),
        # Filter with string operations - accept engine's null propagation
        pytest.param(
            [
                {"name": "included"},
                {"name": "excluded"},
                {"name": None},
            ],
            col("name").is_not_null() & (col("name") != "excluded"),
            [True, False, False],
            "string_filter",
        ),
    ],
)
def test_with_column_null_checks_and_membership_operations(
    ray_start_regular_shared,
    test_data,
    expression,
    expected_results,
    test_description,
    target_max_block_size_infinite_or_default,
):
    """Test null checking, is_in/not_in membership operations, and string comparisons with with_column."""
    ds = ray.data.from_items(test_data)
    result_ds = ds.with_column("result", expression)

    # Convert to pandas and assert on the whole dataframe
    result_df = result_ds.to_pandas()

    # Create expected dataframe from test data
    expected_data = {}
    for key in test_data[0].keys():
        expected_data[key] = [row[key] for row in test_data]
    expected_data["result"] = expected_results

    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "expression_factory, expected_results, test_description",
    [
        # Complex boolean expressions
        pytest.param(
            lambda: (col("age") > 18) & (col("country") == "USA"),
            [
                True,
                False,
                False,
            ],  # [(25>18)&("USA"=="USA"), (17>18)&("Canada"=="USA"), (30>18)&("UK"=="USA")]
            "complex_and_expression",
        ),
        pytest.param(
            lambda: (col("age") < 18) | (col("country") == "USA"),
            [
                True,
                True,
                False,
            ],  # [(25<18)|("USA"=="USA"), (17<18)|("Canada"=="USA"), (30<18)|("UK"=="USA")]
            "complex_or_expression",
        ),
        pytest.param(
            lambda: ~((col("age") < 25) & (col("country") != "USA")),
            [
                True,
                False,
                True,
            ],  # ~[(25<25)&("USA"!="USA"), (17<25)&("Canada"!="USA"), (30<25)&("UK"!="USA")]
            "complex_not_expression",
        ),
        # Age group calculation (common use case)
        pytest.param(
            lambda: col("age") // 10 * 10,
            [20, 10, 30],  # [25//10*10, 17//10*10, 30//10*10]
            "age_group_calculation",
        ),
        # Eligibility flags
        pytest.param(
            lambda: (col("age") >= 21)
            & (col("score") >= 10)
            & col("active").is_not_null()
            & (col("active") == lit(True)),
            [
                True,
                False,
                False,
            ],
            "eligibility_flag",
        ),
    ],
)
def test_with_column_complex_boolean_expressions(
    ray_start_regular_shared,
    expression_factory,
    expected_results,
    test_description,
    target_max_block_size_infinite_or_default,
):
    """Test complex boolean expressions with AND, OR, NOT operations commonly used for filtering and flagging."""
    test_data = [
        {"age": 25, "country": "USA", "active": True, "score": 20},
        {"age": 17, "country": "Canada", "active": False, "score": 10},
        {"age": 30, "country": "UK", "active": None, "score": 20},
    ]

    ds = ray.data.from_items(test_data)
    expression = expression_factory()
    result_ds = ds.with_column("result", expression)

    # Convert to pandas and assert on the whole dataframe
    result_df = result_ds.to_pandas()
    expected_df = pd.DataFrame(
        {
            "age": [25, 17, 30],
            "country": ["USA", "Canada", "UK"],
            "active": [True, False, None],
            "score": [20, 10, 20],
            "result": expected_results,
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_chained_expression_operations(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test chaining multiple expression operations together in a data transformation pipeline."""
    test_data = [
        {"age": 25, "salary": 50000, "active": True, "score": 20},
        {"age": 17, "salary": 0, "active": False, "score": 10},
        {"age": 35, "salary": 75000, "active": None, "score": 20},
    ]

    ds = ray.data.from_items(test_data)

    # Chain multiple operations
    result_ds = (
        ds.with_column("is_adult", col("age") >= 18)
        .with_column("age_group", (col("age") // 10) * 10)
        .with_column("has_salary", col("salary") != 0)
        .with_column(
            "is_active_adult", (col("age") >= 18) & col("active").is_not_null()
        )
        .with_column("salary_tier", (col("salary") // 25000) * 25000)
        .with_column("score_tier", (col("score") // 20) * 20)
    )

    # Convert to pandas and assert on the whole dataframe
    result_df = result_ds.to_pandas()
    expected_df = pd.DataFrame(
        {
            "age": [25, 17, 35],
            "salary": [50000, 0, 75000],
            "active": [True, False, None],
            "score": [20, 10, 20],  # Add the missing score column
            "is_adult": [True, False, True],
            "age_group": [20, 10, 30],  # age // 10 * 10
            "has_salary": [True, False, True],  # salary != 0
            "is_active_adult": [
                True,
                False,
                False,
            ],  # (age >= 18) & (active is not null)
            "salary_tier": [50000, 0, 75000],  # salary // 25000 * 25000
            "score_tier": [20, 0, 20],  # score // 20 * 20
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "filter_expr, test_data, expected_flags, test_description",
    [
        # Simple filter expressions
        pytest.param(
            col("age") >= 21,
            [
                {"age": 20, "name": "Alice"},
                {"age": 21, "name": "Bob"},
                {"age": 25, "name": "Charlie"},
            ],
            [False, True, True],
            "age_filter",
        ),
        pytest.param(
            col("score") > 50,
            [
                {"score": 30, "status": "fail"},
                {"score": 50, "status": "pass"},
                {"score": 70, "status": "pass"},
            ],
            [False, False, True],
            "score_filter",
        ),
        # Complex filter with multiple conditions
        pytest.param(
            (col("age") >= 18) & col("active"),
            [
                {"age": 17, "active": True},
                {"age": 18, "active": False},
                {"age": 25, "active": True},
            ],
            [False, False, True],
            "complex_and_filter",
        ),
        pytest.param(
            (col("status") == "approved") | (col("priority") == "high"),
            [
                {"status": "pending", "priority": "low"},
                {"status": "approved", "priority": "low"},
                {"status": "pending", "priority": "high"},
            ],
            [False, True, True],
            "complex_or_filter",
        ),
        # Filter with null handling
        pytest.param(
            col("value").is_not_null() & (col("value") > 0),
            [
                {"value": None},
                {"value": -5},
                {"value": 10},
            ],
            [
                False,
                False,
                True,
            ],
            "null_aware_filter",
        ),
        # Filter with string operations - reorder to check null first
        pytest.param(
            col("name").is_not_null() & (col("name") != "excluded"),
            [
                {"name": "included"},
                {"name": "excluded"},
                {"name": None},
            ],
            [True, False, False],
            "string_filter",
        ),
        # Filter with membership operations
        pytest.param(
            col("category").is_in(["A", "B"]),
            [
                {"category": "A"},
                {"category": "B"},
                {"category": "C"},
                {"category": "D"},
            ],
            [True, True, False, False],
            "membership_filter",
        ),
        # Nested filter expressions
        pytest.param(
            (col("score") >= 50) & (col("grade") != "F"),
            [
                {"score": 45, "grade": "F"},
                {"score": 55, "grade": "D"},
                {"score": 75, "grade": "B"},
                {"score": 30, "grade": "F"},
            ],
            [False, True, True, False],
            "nested_filters",
        ),
    ],
)
def test_with_column_filter_expressions(
    ray_start_regular_shared,
    filter_expr,
    test_data,
    expected_flags,
    test_description,
):
    """Test filter() expression functionality with with_column for creating boolean flag columns."""
    ds = ray.data.from_items(test_data)
    result_ds = ds.with_column("is_filtered", filter_expr)

    # Convert to pandas and verify the filter results
    result_df = result_ds.to_pandas()

    # Build expected dataframe
    expected_df = pd.DataFrame(test_data)
    expected_df["is_filtered"] = expected_flags

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_filter_in_pipeline(ray_start_regular_shared):
    """Test filter() expressions used in a data processing pipeline with multiple transformations."""
    # Create test data for a sales analysis pipeline
    test_data = [
        {"product": "A", "quantity": 10, "price": 100, "region": "North"},
        {"product": "B", "quantity": 5, "price": 200, "region": "South"},
        {"product": "C", "quantity": 20, "price": 50, "region": "North"},
        {"product": "D", "quantity": 15, "price": 75, "region": "East"},
        {"product": "E", "quantity": 3, "price": 300, "region": "West"},
    ]

    ds = ray.data.from_items(test_data)

    # Build a pipeline with multiple filter expressions
    result_ds = (
        ds
        # Calculate total revenue
        .with_column("revenue", col("quantity") * col("price"))
        # Flag high-value transactions
        .with_column("is_high_value", col("revenue") >= 1000)
        # Flag bulk orders
        .with_column("is_bulk_order", col("quantity") >= 10)
        # Flag premium products
        .with_column("is_premium", col("price") >= 100)
        # Create composite filter for special handling
        .with_column(
            "needs_special_handling",
            (col("is_high_value")) | (col("is_bulk_order") & col("is_premium")),
        )
        # Regional filter
        .with_column("is_north_region", col("region") == "North")
    )

    # Convert to pandas and verify
    result_df = result_ds.to_pandas()

    expected_df = pd.DataFrame(
        {
            "product": ["A", "B", "C", "D", "E"],
            "quantity": [10, 5, 20, 15, 3],
            "price": [100, 200, 50, 75, 300],
            "region": ["North", "South", "North", "East", "West"],
            "revenue": [1000, 1000, 1000, 1125, 900],
            "is_high_value": [True, True, True, True, False],
            "is_bulk_order": [True, False, True, True, False],
            "is_premium": [True, True, False, False, True],
            "needs_special_handling": [True, True, True, True, False],
            "is_north_region": [True, False, True, False, False],
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.parametrize(
    "expr_factory, expected_columns, alias_name, expected_values",
    [
        (
            lambda: col("id").alias("new_id"),
            ["id", "new_id"],
            "new_id",
            [0, 1, 2, 3, 4],  # Copy of id column
        ),
        (
            lambda: (col("id") + 1).alias("id_plus_one"),
            ["id", "id_plus_one"],
            "id_plus_one",
            [1, 2, 3, 4, 5],  # id + 1
        ),
        (
            lambda: (col("id") * 2 + 5).alias("transformed"),
            ["id", "transformed"],
            "transformed",
            [5, 7, 9, 11, 13],  # id * 2 + 5
        ),
        (
            lambda: lit(42).alias("constant"),
            ["id", "constant"],
            "constant",
            [42, 42, 42, 42, 42],  # lit(42)
        ),
        (
            lambda: (col("id") >= 0).alias("is_non_negative"),
            ["id", "is_non_negative"],
            "is_non_negative",
            [True, True, True, True, True],  # id >= 0
        ),
        (
            lambda: (col("id") + 1).alias("id"),
            ["id"],  # Only one column since we're overwriting id
            "id",
            [1, 2, 3, 4, 5],  # id + 1 replaces original id
        ),
    ],
    ids=[
        "col_alias",
        "arithmetic_alias",
        "complex_alias",
        "literal_alias",
        "comparison_alias",
        "overwrite_existing_column",
    ],
)
def test_with_column_alias_expressions(
    ray_start_regular_shared,
    expr_factory,
    expected_columns,
    alias_name,
    expected_values,
):
    """Test that alias expressions work correctly with with_column."""
    expr = expr_factory()

    # Verify the alias name matches what we expect
    assert expr.name == alias_name

    # Apply the aliased expression
    ds = ray.data.range(5).with_column(alias_name, expr)

    # Convert to pandas for comprehensive comparison
    result_df = ds.to_pandas()

    # Create expected DataFrame
    expected_df = pd.DataFrame({"id": [0, 1, 2, 3, 4], alias_name: expected_values})

    # Ensure column order matches expected_columns
    expected_df = expected_df[expected_columns]

    # Assert the entire DataFrame is equal
    pd.testing.assert_frame_equal(result_df, expected_df)
    # Verify the alias expression evaluates the same as the non-aliased version
    non_aliased_expr = expr
    ds_non_aliased = ray.data.range(5).with_column(alias_name, non_aliased_expr)

    non_aliased_df = ds_non_aliased.to_pandas()

    pd.testing.assert_frame_equal(result_df, non_aliased_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
