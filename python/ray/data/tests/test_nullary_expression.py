import uuid as uuid_module

import numpy as np
import pytest

import ray
from ray.data.expressions import NullaryExpr, NullaryOperation, col, random, uuid
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_random_expression_creation():
    """Test that random() creates a NullaryExpr with RANDOM operation."""
    # Test without seed
    expr = random()
    assert isinstance(expr, NullaryExpr)
    assert expr.op == NullaryOperation.RANDOM
    assert expr.kwargs.get("seed") is None
    assert expr.kwargs.get("reseed_after_execution", True) is True

    # Test with seed
    expr = random(seed=42)
    assert isinstance(expr, NullaryExpr)
    assert expr.op == NullaryOperation.RANDOM
    assert expr.kwargs.get("seed") == 42
    assert expr.kwargs.get("reseed_after_execution", True) is True

    # Test with seed and reseed_after_execution=False
    expr = random(seed=42, reseed_after_execution=False)
    assert isinstance(expr, NullaryExpr)
    assert expr.op == NullaryOperation.RANDOM
    assert expr.kwargs.get("seed") == 42
    assert expr.kwargs.get("reseed_after_execution", True) is False


@pytest.mark.parametrize(
    "seed1,seed2,reseed1,reseed2,expected_equal",
    [
        (42, 42, True, True, True),
        (42, 123, True, True, False),
        (None, None, True, True, True),
        (None, None, True, False, False),
        (42, None, True, True, False),
        (42, 42, True, False, False),
        (42, 42, False, False, True),
    ],
)
def test_random_expression_structural_equality(
    seed1, seed2, reseed1, reseed2, expected_equal
):
    """Test structural equality comparison for random expressions."""
    expr1 = random(seed=seed1, reseed_after_execution=reseed1)
    expr2 = random(seed=seed2, reseed_after_execution=reseed2)

    assert expr1.structurally_equals(expr2) == expected_equal
    assert expr2.structurally_equals(expr1) == expected_equal


def test_random_expression_structural_equality_with_non_random_expr():
    """Test structural equality comparison with non-nullary expression."""
    random_expr = random(seed=42)
    non_random_expr = col("id")

    assert not random_expr.structurally_equals(non_random_expr)
    assert not non_random_expr.structurally_equals(random_expr)


def test_random_values_range(ray_start_regular_shared):
    """Test that random values are in range [0, 1)."""
    ds = ray.data.range(1000).with_column("rand", random())
    results = ds.take_all()

    assert len(results) == 1000
    for result in results:
        assert isinstance(result["rand"], (float, np.floating))
        assert 0.0 <= result["rand"] < 1.0
        assert "id" in result  # Verify other columns are preserved


def test_random_without_seed_non_deterministic(ray_start_regular_shared):
    """Test that without seed produces non-deterministic values."""
    ds1 = ray.data.range(100).with_column("rand", random())
    ds2 = ray.data.range(100).with_column("rand", random())

    values1 = [r["rand"] for r in ds1.take_all()]
    values2 = [r["rand"] for r in ds2.take_all()]

    # Should be different (very unlikely to be identical)
    assert values1 != values2


@pytest.mark.parametrize("seed", [0, 42, 123])
@pytest.mark.parametrize("num_blocks", [None, 1, 4, 8])
def test_random_with_seed_deterministic(ray_start_regular_shared, seed, num_blocks):
    """Test that with seed produces deterministic values."""
    kwargs = {"override_num_blocks": num_blocks} if num_blocks is not None else {}
    ds1 = ray.data.range(100, **kwargs).with_column("rand", random(seed=seed))
    ds2 = ray.data.range(100, **kwargs).with_column("rand", random(seed=seed))

    values1 = [r["rand"] for r in ds1.take_all()]
    values2 = [r["rand"] for r in ds2.take_all()]

    assert values1 == values2


@pytest.mark.parametrize("batch_format", ["default", "pandas", "pyarrow"])
def test_random_with_different_batch_formats(ray_start_regular_shared, batch_format):
    """Test random expression works with different batch formats."""
    import pandas as pd
    import pyarrow as pa

    ds = ray.data.range(100).with_column("rand", random(seed=42))

    all_values = []
    for batch in ds.iter_batches(batch_format=batch_format):
        rand_col = batch["rand"]
        if batch_format == "pandas" and isinstance(rand_col, pd.Series):
            all_values.extend(rand_col.tolist())
        elif batch_format == "pyarrow" and isinstance(rand_col, pa.ChunkedArray):
            all_values.extend(rand_col.to_pylist())  # type: ignore
        else:
            all_values.extend(
                list(rand_col) if hasattr(rand_col, "__iter__") else [rand_col]
            )

    assert len(all_values) == 100
    for val in all_values:
        assert 0.0 <= val < 1.0


@pytest.mark.parametrize("op", [random, uuid])
def test_nullary_empty_dataset(ray_start_regular_shared, op):
    """Test nullary expression with empty dataset."""
    ds = ray.data.range(0).with_column("col", op())
    assert len(ds.take_all()) == 0


@pytest.mark.parametrize(
    "reseed_after_execution,expected_all_equal",
    [
        (True, False),
        (False, True),
    ],
)
def test_reproducibility_across_epochs(
    ray_start_regular_shared, reseed_after_execution, expected_all_equal
):
    """Test reproducibility across multiple iter_batches() epochs."""
    ds = ray.data.range(100).with_column(
        "rand", random(seed=42, reseed_after_execution=reseed_after_execution)
    )

    # Collect values from multiple epochs
    epoch_values = []
    for _ in range(3):
        values = []
        for batch in ds.iter_batches():
            rand_col = batch["rand"]
            if isinstance(rand_col, np.ndarray):
                values.extend(rand_col.tolist())
            else:
                values.extend(list(rand_col))
        epoch_values.append(values)

    if expected_all_equal:
        # Same across epochs when reseed_after_execution=False
        assert epoch_values[0] == epoch_values[1]
        assert epoch_values[1] == epoch_values[2]
    else:
        # Different across epochs when reseed_after_execution=True
        assert epoch_values[0] != epoch_values[1]
        assert epoch_values[1] != epoch_values[2]


@pytest.mark.parametrize("num_blocks1,num_blocks2", [(1, 4), (4, 8)])
def test_different_num_blocks_produces_different_values(
    ray_start_regular_shared, num_blocks1, num_blocks2
):
    """Test that different num_blocks with same seed produces different values."""
    ds1 = ray.data.range(100, override_num_blocks=num_blocks1).with_column(
        "rand", random(seed=42)
    )
    ds2 = ray.data.range(100, override_num_blocks=num_blocks2).with_column(
        "rand", random(seed=42)
    )

    values1 = [r["rand"] for r in ds1.take_all()]
    values2 = [r["rand"] for r in ds2.take_all()]

    # Should be different (expected behavior due to different task_idx)
    assert values1 != values2


@pytest.mark.parametrize(
    "args,kwargs,expected_error,error_message",
    [
        # Too many positional arguments
        (
            (1, 2, 3),
            {},
            TypeError,
            "random\\(\\) takes from 0 to 2 positional arguments but 3 were given",
        ),
        # Keyword "seed" with 1 positional arg (duplicate)
        (
            (42,),
            {"seed": 123},
            TypeError,
            "random\\(\\) got multiple values for argument 'seed'",
        ),
        # Unexpected keyword argument
        (
            (),
            {"invalid_arg": 42},
            TypeError,
            "random\\(\\) got an unexpected keyword argument 'invalid_arg'",
        ),
        (
            (),
            {"seed": 42, "invalid_arg": True},
            TypeError,
            "random\\(\\) got an unexpected keyword argument 'invalid_arg'",
        ),
    ],
)
def test_random_validation_errors(
    ray_start_regular_shared, args, kwargs, expected_error, error_message
):
    """Test that random() validates arguments correctly."""
    with pytest.raises(expected_error, match=error_message):
        random(*args, **kwargs)


def test_uuid_expression_creation():
    """Test that uuid() creates a NullaryExpr with UUID operation."""
    expr = uuid()
    assert isinstance(expr, NullaryExpr)
    assert expr.op == NullaryOperation.UUID
    assert expr.kwargs == {}


def test_uuid_expression_structural_equality():
    """Test structural equality comparison for uuid expressions."""
    expr1 = uuid()
    expr2 = uuid()

    # All uuid() expressions should be structurally equal (no parameters)
    assert expr1.structurally_equals(expr2)
    assert expr2.structurally_equals(expr1)


def test_uuid_expression_structural_equality_with_non_uuid_expr():
    """Test structural equality comparison with non-nullary expression."""
    uuid_expr = uuid()
    non_uuid_expr = col("id")

    assert not uuid_expr.structurally_equals(non_uuid_expr)
    assert not non_uuid_expr.structurally_equals(uuid_expr)


def test_uuid_values_format(ray_start_regular_shared):
    """Test that uuid values are valid UUID strings."""
    ds = ray.data.range(100).with_column("uuid_col", uuid())
    results = ds.take_all()

    assert len(results) == 100
    for result in results:
        uuid_value = result["uuid_col"]
        assert isinstance(uuid_value, str)
        # Validate UUID format
        uuid_module.UUID(uuid_value)  # Will raise ValueError if invalid
        assert "id" in result  # Verify other columns are preserved


def test_uuid_values_unique(ray_start_regular_shared):
    """Test that uuid values are unique."""
    ds = ray.data.range(1000).with_column("uuid_col", uuid())
    results = ds.take_all()

    uuid_values = [r["uuid_col"] for r in results]
    # All UUIDs should be unique
    assert len(uuid_values) == len(set(uuid_values))


@pytest.mark.parametrize("batch_format", ["default", "pandas", "pyarrow"])
def test_uuid_with_different_batch_formats(ray_start_regular_shared, batch_format):
    """Test uuid expression works with different batch formats."""
    import pandas as pd
    import pyarrow as pa

    ds = ray.data.range(100).with_column("uuid_col", uuid())

    all_values = []
    for batch in ds.iter_batches(batch_format=batch_format):
        uuid_col = batch["uuid_col"]
        if batch_format == "pandas" and isinstance(uuid_col, pd.Series):
            all_values.extend(uuid_col.tolist())
        elif batch_format == "pyarrow" and isinstance(uuid_col, pa.ChunkedArray):
            all_values.extend(uuid_col.to_pylist())  # type: ignore
        else:
            all_values.extend(
                list(uuid_col) if hasattr(uuid_col, "__iter__") else [uuid_col]
            )

    assert len(all_values) == 100
    for val in all_values:
        assert isinstance(val, str)
        uuid_module.UUID(val)  # Validate UUID format


@pytest.mark.parametrize(
    "args,kwargs,expected_error,error_message",
    [
        # Too many positional arguments
        (
            (1,),
            {},
            TypeError,
            "uuid\\(\\) takes 0 positional arguments but 1 was given",
        ),
        # Unexpected keyword argument
        (
            (),
            {"invalid_arg": 42},
            TypeError,
            "uuid\\(\\) got an unexpected keyword argument 'invalid_arg'",
        ),
    ],
)
def test_uuid_validation_errors(
    ray_start_regular_shared, args, kwargs, expected_error, error_message
):
    """Test that uuid() validates arguments correctly."""
    with pytest.raises(expected_error, match=error_message):
        uuid(*args, **kwargs)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
