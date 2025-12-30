import math

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pkg_resources import parse_version

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data._internal.util import rows_same
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
    "expr_factory, expected_values",
    [
        pytest.param(lambda: col("value").ceil(), [-1, 0, 0, 1, 2], id="ceil"),
        pytest.param(lambda: col("value").floor(), [-2, -1, 0, 0, 1], id="floor"),
        pytest.param(lambda: col("value").round(), [-2, 0, 0, 0, 2], id="round"),
        pytest.param(lambda: col("value").trunc(), [-1, 0, 0, 0, 1], id="trunc"),
    ],
)
def test_with_column_rounding_operations(
    ray_start_regular_shared,
    expr_factory,
    expected_values,
):
    """Test ceil, floor, round, and trunc expressions."""
    values = [-1.75, -0.25, 0.0, 0.25, 1.75]
    ds = ray.data.from_items([{"value": v} for v in values])
    expr = expr_factory()
    result_df = ds.with_column("result", expr).to_pandas()
    expected_df = pd.DataFrame({"value": values, "result": expected_values})
    assert rows_same(result_df, expected_df)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "expr_factory, expected_fn",
    [
        pytest.param(lambda: col("value").ln(), math.log, id="ln"),
        pytest.param(lambda: col("value").log10(), math.log10, id="log10"),
        pytest.param(lambda: col("value").log2(), math.log2, id="log2"),
        pytest.param(lambda: col("value").exp(), math.exp, id="exp"),
    ],
)
@pytest.mark.parametrize(
    "input_values",
    [
        pytest.param([1.0, math.e, 10.0, 4.0], id="float_inputs"),
        pytest.param([1, 10, 4, 2], id="int_inputs"),
    ],
)
def test_with_column_logarithmic_operations(
    ray_start_regular_shared,
    expr_factory,
    expected_fn,
    input_values,
):
    """Test logarithmic and exponential expressions."""
    values = input_values
    ds = ray.data.from_items([{"value": v} for v in values])
    expr = expr_factory()
    expected_values = [expected_fn(v) for v in values]
    result_df = ds.with_column("result", expr).to_pandas()
    expected_df = pd.DataFrame({"value": values, "result": expected_values})
    assert rows_same(result_df, expected_df)


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


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_callable_class_udf_actor_semantics(ray_start_regular_shared):
    """Test that callable class UDFs maintain state across batches using actor semantics."""
    import pyarrow.compute as pc

    # Create a callable class UDF that tracks the number of times it's called
    @udf(return_dtype=DataType.int32())
    class InvocationCounter:
        def __init__(self, offset=0):
            self.offset = offset
            self.call_count = 0

        def __call__(self, x):
            # Increment call count each time the UDF is invoked
            self.call_count += 1
            # Add the offset plus the call count to show state is maintained
            return pc.add(pc.add(x, self.offset), self.call_count)

    # Create a dataset with multiple blocks to ensure multiple invocations
    ds = ray.data.range(20, override_num_blocks=4)

    # Use the callable class UDF
    counter = InvocationCounter(offset=100)
    result_ds = ds.with_column("result", counter(col("id")))

    # Convert to list to trigger execution
    results = result_ds.take_all()

    # The results should show that the call_count incremented across batches
    # Since we have 4 blocks, the UDF should be called 4 times on the same actor
    # The exact values will depend on which batch each row came from
    # But we can verify that the offset (100) was applied
    for result in results:
        # Each result should have the base id + offset (100) + at least 1 (first call)
        assert result["result"] >= result["id"] + 100 + 1


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_callable_class_udf_with_constructor_args(
    ray_start_regular_shared,
):
    """Test that callable class UDFs correctly use constructor arguments."""
    import pyarrow.compute as pc

    @udf(return_dtype=DataType.int32())
    class AddOffset:
        def __init__(self, offset):
            self.offset = offset

        def __call__(self, x):
            return pc.add(x, self.offset)

    # Create dataset
    ds = ray.data.range(10)

    # Test with different offsets
    add_five = AddOffset(5)
    add_ten = AddOffset(10)

    result_5 = ds.with_column("plus_five", add_five(col("id"))).to_pandas()
    result_10 = ds.with_column("plus_ten", add_ten(col("id"))).to_pandas()

    # Verify the offsets were applied correctly
    expected_5 = pd.DataFrame({"id": list(range(10)), "plus_five": list(range(5, 15))})
    expected_10 = pd.DataFrame({"id": list(range(10)), "plus_ten": list(range(10, 20))})

    assert rows_same(result_5, expected_5)
    assert rows_same(result_10, expected_10)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_multiple_callable_class_udfs(ray_start_regular_shared):
    """Test that multiple callable class UDFs can be used in the same projection."""
    import pyarrow.compute as pc

    @udf(return_dtype=DataType.int32())
    class Multiplier:
        def __init__(self, factor):
            self.factor = factor

        def __call__(self, x):
            return pc.multiply(x, self.factor)

    @udf(return_dtype=DataType.int32())
    class Adder:
        def __init__(self, addend):
            self.addend = addend

        def __call__(self, x):
            return pc.add(x, self.addend)

    # Create dataset
    ds = ray.data.range(5)

    # Use multiple callable class UDFs
    times_two = Multiplier(2)
    plus_ten = Adder(10)

    result = ds.with_column("doubled", times_two(col("id"))).with_column(
        "plus_ten", plus_ten(col("id"))
    )

    result_df = result.to_pandas()
    expected_df = pd.DataFrame(
        {
            "id": [0, 1, 2, 3, 4],
            "doubled": [0, 2, 4, 6, 8],
            "plus_ten": [10, 11, 12, 13, 14],
        }
    )

    assert rows_same(result_df, expected_df)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_same_callable_class_different_constructor_args(
    ray_start_regular_shared,
):
    """Test that the same callable class with different constructor args works correctly.

    This test ensures that when the same callable class is instantiated with different
    constructor arguments, each instance maintains its own state. This is important for
    future-proofing in case Actor->Actor fusion becomes enabled.
    """
    import pyarrow.compute as pc

    @udf(return_dtype=DataType.int32())
    class Multiplier:
        def __init__(self, factor):
            self.factor = factor

        def __call__(self, x):
            return pc.multiply(x, self.factor)

    # Create dataset
    ds = ray.data.range(5)

    # Use the SAME class with DIFFERENT constructor arguments
    times_two = Multiplier(2)
    times_three = Multiplier(3)

    result = ds.with_column("times_two", times_two(col("id"))).with_column(
        "times_three", times_three(col("id"))
    )

    print(result.explain())

    result_df = result.to_pandas()
    expected_df = pd.DataFrame(
        {
            "id": [0, 1, 2, 3, 4],
            "times_two": [0, 2, 4, 6, 8],  # id * 2
            "times_three": [0, 3, 6, 9, 12],  # id * 3
        }
    )

    from ray.data._internal.util import rows_same

    assert rows_same(result_df, expected_df)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_callable_class_udf_with_compute_strategy(
    ray_start_regular_shared,
):
    """Test that compute strategy can be specified for callable class UDFs."""
    import pyarrow.compute as pc

    @udf(return_dtype=DataType.int32())
    class AddOffset:
        def __init__(self, offset):
            self.offset = offset

        def __call__(self, x):
            return pc.add(x, self.offset)

    # Create dataset
    ds = ray.data.range(10)

    # Use a specific compute strategy
    add_five = AddOffset(5)
    result = ds.with_column(
        "result",
        add_five(col("id")),
        compute=ray.data.ActorPoolStrategy(size=2),
    )

    result_df = result.to_pandas()
    expected_df = pd.DataFrame({"id": list(range(10)), "result": list(range(5, 15))})

    assert rows_same(result_df, expected_df)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_async_callable_class_udf(ray_start_regular_shared):
    """Test that async callable class UDFs work correctly with actor semantics."""
    import asyncio

    import pyarrow.compute as pc

    @udf(return_dtype=DataType.int32())
    class AsyncAddOffset:
        def __init__(self, offset):
            self.offset = offset
            self.call_count = 0

        async def __call__(self, x):
            # Simulate async work
            await asyncio.sleep(0.001)
            self.call_count += 1
            # Add offset to show the UDF was called
            return pc.add(x, self.offset)

    # Create dataset
    ds = ray.data.range(10, override_num_blocks=2)

    # Use async callable class UDF
    add_five = AsyncAddOffset(5)
    result = ds.with_column("result", add_five(col("id")))

    result_df = result.to_pandas()
    expected_df = pd.DataFrame({"id": list(range(10)), "result": list(range(5, 15))})

    assert rows_same(result_df, expected_df)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_async_callable_class_udf_with_state(ray_start_regular_shared):
    """Test that async callable class UDFs maintain state across batches."""
    import asyncio

    import pyarrow.compute as pc

    @udf(return_dtype=DataType.int32())
    class AsyncCounter:
        def __init__(self):
            self.total_processed = 0

        async def __call__(self, x):
            # Simulate async work
            await asyncio.sleep(0.001)
            # Track how many items we've processed
            batch_size = len(x)
            self.total_processed += batch_size
            # Return the running count
            return pc.add(x, self.total_processed - batch_size)

    # Create dataset with multiple blocks
    ds = ray.data.range(20, override_num_blocks=4)

    # Use async callable class UDF with state
    counter = AsyncCounter()
    result = ds.with_column("running_total", counter(col("id")))

    # Just verify we got results without errors
    # The exact values will depend on execution order
    results = result.take_all()
    assert len(results) == 20
    # All values should be at least the original id
    for i, result in enumerate(results):
        assert result["running_total"] >= result["id"]


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_multiple_async_callable_class_udfs(ray_start_regular_shared):
    """Test that multiple async callable class UDFs can work together."""
    import asyncio

    import pyarrow.compute as pc

    @udf(return_dtype=DataType.int32())
    class AsyncMultiplier:
        def __init__(self, factor):
            self.factor = factor

        async def __call__(self, x):
            await asyncio.sleep(0.001)
            return pc.multiply(x, self.factor)

    @udf(return_dtype=DataType.int32())
    class AsyncAdder:
        def __init__(self, addend):
            self.addend = addend

        async def __call__(self, x):
            await asyncio.sleep(0.001)
            return pc.add(x, self.addend)

    # Create dataset
    ds = ray.data.range(5)

    # Use multiple async callable class UDFs
    times_two = AsyncMultiplier(2)
    plus_ten = AsyncAdder(10)

    result = ds.with_column("doubled", times_two(col("id"))).with_column(
        "plus_ten", plus_ten(col("id"))
    )

    result_df = result.to_pandas()
    expected_df = pd.DataFrame(
        {
            "id": [0, 1, 2, 3, 4],
            "doubled": [0, 2, 4, 6, 8],
            "plus_ten": [10, 11, 12, 13, 14],
        }
    )

    assert rows_same(result_df, expected_df)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_async_generator_udf_multiple_yields(ray_start_regular_shared):
    """Test that async generator UDFs correctly handle multiple yields.

    When an async generator UDF yields multiple values, the last (most recent)
    value is returned. This matches map_batches behavior of collecting all yields,
    while adapting to expression context where a single value per row is required.
    """

    import pyarrow.compute as pc

    @udf(return_dtype=DataType.int32())
    class AsyncGeneratorMultiYield:
        """UDF that yields multiple values - last yield is returned."""

        def __init__(self, offset):
            self.offset = offset

        async def __call__(self, x):
            # Yield multiple values for the same input
            # Fix: Last yield is returned (most recent/final result)
            yield pc.add(x, self.offset)  # First yield: x + offset
            yield pc.multiply(x, self.offset + 10)  # Second yield: x * (offset + 10)
            yield pc.add(x, self.offset * 2)  # Third yield: x + (offset * 2) - RETURNED

    # Create dataset
    ds = ray.data.range(5, override_num_blocks=1)

    # Use async generator UDF
    udf_instance = AsyncGeneratorMultiYield(5)
    result = ds.with_column("result", udf_instance(col("id")))

    result_df = result.to_pandas()

    # Fixed behavior: last yield is returned
    # Input: [0, 1, 2, 3, 4]
    # First yield: [0+5, 1+5, 2+5, 3+5, 4+5] = [5, 6, 7, 8, 9]
    # Second yield: [0*15, 1*15, 2*15, 3*15, 4*15] = [0, 15, 30, 45, 60]
    # Third yield (RETURNED): [0+10, 1+10, 2+10, 3+10, 4+10] = [10, 11, 12, 13, 14]

    expected_after_fix = pd.DataFrame(
        {
            "id": [0, 1, 2, 3, 4],
            "result": [10, 11, 12, 13, 14],  # Last yield returned: id + (5*2) = id + 10
        }
    )

    assert rows_same(result_df, expected_after_fix)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
