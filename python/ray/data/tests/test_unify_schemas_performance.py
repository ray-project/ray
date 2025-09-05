import pyarrow as pa
import pytest

from ray.data._internal.arrow_ops.transform_pyarrow import (
    unify_schemas,
)
from ray.data.extensions import (
    ArrowPythonObjectType,
    ArrowTensorType,
    ArrowVariableShapedTensorType,
)


# Schema factory functions - just return schemas
def _create_simple_schema(num_columns):
    return pa.schema([(f"col_{i}", pa.int64()) for i in range(num_columns)])


def _create_tensor_fixed_schema(num_columns):
    return pa.schema(
        [
            (f"tensor_{i}", ArrowTensorType((2, 2), pa.float32()))
            for i in range(num_columns)
        ]
    )


def _create_tensor_variable_schema(num_columns):
    return pa.schema(
        [
            (f"tensor_{i}", ArrowVariableShapedTensorType(pa.float32(), 2))
            for i in range(num_columns)
        ]
    )


def _create_object_schema(num_columns):
    return pa.schema(
        [(f"obj_{i}", ArrowPythonObjectType()) for i in range(num_columns)]
    )


def _create_nested_struct_schema(num_columns):
    fields = []
    for i in range(num_columns):
        inner_struct = pa.struct(
            [("x", pa.int32()), ("y", pa.string()), ("z", pa.float64())]
        )
        fields.append((f"struct_{i}", inner_struct))
    return pa.schema(fields)


def _create_deep_nested_schema(num_columns):
    fields = []
    for i in range(num_columns):
        level4 = pa.struct([("data", pa.int32()), ("meta", pa.string())])
        level3 = pa.struct([("level4", level4), ("id3", pa.int64())])
        level2 = pa.struct([("level3", level3), ("id2", pa.int64())])
        level1 = pa.struct([("level2", level2), ("id1", pa.int64())])
        fields.append((f"deep_{i}", level1))
    return pa.schema(fields)


def _create_mixed_complex_schema(num_columns):
    fields = []
    for i in range(num_columns):
        field_type = i % 5
        if field_type == 0:
            fields.append((f"col_{i}", pa.int64()))
        elif field_type == 1:
            fields.append((f"col_{i}", ArrowTensorType((3, 3), pa.int32())))
        elif field_type == 2:
            fields.append((f"col_{i}", ArrowPythonObjectType()))
        elif field_type == 3:
            inner_struct = pa.struct([("a", pa.int32()), ("b", pa.string())])
            fields.append((f"col_{i}", inner_struct))
        else:
            fields.append((f"col_{i}", pa.list_(pa.float64())))
    return pa.schema(fields)


@pytest.mark.parametrize("num_schemas", [10, 100])
@pytest.mark.parametrize("num_columns", [10, 100, 1000, 5000])
@pytest.mark.parametrize(
    "schema_factory,expected_time_per_schema_per_column",
    [
        (_create_simple_schema, 0.00001),
        (_create_tensor_fixed_schema, 0.00005),
        (_create_tensor_variable_schema, 0.00005),
        (_create_object_schema, 0.00005),
        (_create_nested_struct_schema, 0.0001),
        (_create_deep_nested_schema, 0.0002),
        (_create_mixed_complex_schema, 0.0002),
    ],
)
def test_unify_schemas_equivalent_performance(
    num_schemas, num_columns, schema_factory, expected_time_per_schema_per_column
):
    """Stress test for unify_schemas when ALL schemas are equivalent (identical).

    This tests the fast path where all schemas are the same and should be optimized
    to return quickly without expensive comparisons.
    """
    import time

    # Create the base schema
    base_schema = schema_factory(num_columns)

    # Create list of identical schemas
    schemas = [base_schema] * num_schemas

    # Time the unification
    start_time = time.time()
    unified = unify_schemas(schemas)
    elapsed_time = time.time() - start_time

    # Verify the result is correct (should be identical to base schema)
    assert unified == base_schema

    # Performance assertions with scaling based on complexity
    scale_factor = num_schemas * num_columns
    max_allowed_time = expected_time_per_schema_per_column * scale_factor
    buffer_factor = 2
    assert elapsed_time < buffer_factor * max_allowed_time, (
        f"unify_schemas took {elapsed_time:.4f}s for {num_schemas} identical "
        f"{schema_factory.__name__} schemas with {num_columns} columns, "
        f"should be < {max_allowed_time:.4f}s"
    )

    # Print timing info for large cases
    if num_schemas >= 1000 or num_columns >= 100:
        print(
            f"\n{schema_factory.__name__}: {num_schemas} schemas x {num_columns} cols = {elapsed_time:.4f}s"
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
