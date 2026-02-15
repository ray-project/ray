import uuid as uuid_module

import numpy as np
import pytest

import ray
from ray.data.expressions import RandomExpr, UUIDExpr, col, random, uuid
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_random_expression_creation():
    """Test that random() creates a RandomExpr with correct fields."""
    # Test without seed
    expr = random()
    assert isinstance(expr, RandomExpr)
    assert expr.seed is None
    assert expr.reseed_after_execution is True

    # Test with seed
    expr = random(seed=42)
    assert isinstance(expr, RandomExpr)
    assert expr.seed == 42
    assert expr.reseed_after_execution is True

    # Test with seed and reseed_after_execution=False
    expr = random(seed=42, reseed_after_execution=False)
    assert isinstance(expr, RandomExpr)
    assert expr.seed == 42
    assert expr.reseed_after_execution is False


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
    """Test structural equality comparison with non-synthetic expression."""
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
def test_synthetic_empty_dataset(ray_start_regular_shared, op):
    """Test synthetic expression with empty dataset."""
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
            "random\\(\\) takes 0 positional arguments but 3 were given",
        ),
        # Keyword "seed" with 1 positional arg (duplicate)
        (
            (42,),
            {"seed": 123},
            TypeError,
            "random\\(\\) takes 0 positional arguments but 1 positional argument",
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


def test_random_multi_block_per_task(ray_start_regular_shared):
    """Test that random values are unique when a single task processes multiple blocks.

    This test verifies the fix for the issue where random() with seed would produce
    duplicate values when a single task processed multiple blocks. Unlike
    monotonically_increasing_id(), which uses a per-task counter in ctx.kwargs to
    differentiate blocks, random() previously had no such mechanism, leading to
    duplicated random data.
    """
    ctx = ray.data.DataContext.get_current()
    original_max_block_size = ctx.target_max_block_size
    try:
        # Set max block size to 32 bytes ~ 4 int64 rows per block.
        # With 5 read tasks of 20 rows each, every task should see 5 blocks.
        ctx.target_max_block_size = 32

        ds = ray.data.range(100, override_num_blocks=5)
        ds = ds.with_column("rand", random(seed=42))
        result = ds.take_all()

        rand_values = [row["rand"] for row in result]
        assert len(rand_values) == 100, f"expected 100 rows, got {len(rand_values)}"
        assert len(rand_values) == len(
            set(rand_values)
        ), "Random values are not unique across blocks within the same task"
    finally:
        ctx.target_max_block_size = original_max_block_size


def test_random_with_column_then_random_shuffle_deterministic(ray_start_regular_shared):
    """Test that random() with seed produces deterministic results even after random_shuffle."""
    from ray.data.expressions import random

    # Create two identical pipelines
    ds1 = ray.data.range(100).with_column("rand", random(seed=42))
    ds1 = ds1.random_shuffle(seed=1)

    ds2 = ray.data.range(100).with_column("rand", random(seed=42))
    ds2 = ds2.random_shuffle(seed=1)

    # The random column values should be deterministic (same seed)
    # but the row order may differ due to shuffle
    results1 = sorted(ds1.take_all(), key=lambda x: x["id"])
    results2 = sorted(ds2.take_all(), key=lambda x: x["id"])

    # Same random values for same id
    for r1, r2 in zip(results1, results2):
        assert r1["rand"] == r2["rand"]


@pytest.mark.parametrize(
    "all_to_all_op,op_kwargs",
    [
        ("random_shuffle", {"seed": 100}),
        ("repartition", {"num_blocks": 5, "shuffle": True}),
        ("sort", {"key": "id"}),
        ("randomize_block_order", {"seed": 100}),
    ],
)
def test_random_reseed_after_execution_with_all_to_all_ops(
    ray_start_regular_shared, all_to_all_op, op_kwargs
):
    """Test that reseed_after_execution works correctly when fused with all-to-all ops.

    This test verifies the fix for the issue where random() expressions with
    reseed_after_execution=True would not properly reseed across epochs when
    fused with all-to-all operations (shuffle, repartition, sort, etc.). The issue
    was that DataContext was not propagated to all-to-all tasks, causing
    execution_idx to always be 0.
    """
    from ray.data.expressions import random

    # Create a dataset with random column
    ds = ray.data.range(10, override_num_blocks=1).with_column(
        "rand", random(seed=42, reseed_after_execution=True)
    )

    # Apply the all-to-all operation
    ds_transformed = getattr(ds, all_to_all_op)(**op_kwargs)

    # First execution
    first_results = sorted(ds_transformed.take_all(), key=lambda x: x["id"])

    # Second execution - should have different random values
    second_results = sorted(ds_transformed.take_all(), key=lambda x: x["id"])

    # Verify random values are different across executions
    first_rand_values = [r["rand"] for r in first_results]
    second_rand_values = [r["rand"] for r in second_results]

    assert first_rand_values != second_rand_values, (
        f"Random values should differ across executions when "
        f"reseed_after_execution=True, even with {all_to_all_op} fusion"
    )

    # Verify the row ids are the same (just checking we have the same data)
    first_ids = [r["id"] for r in first_results]
    second_ids = [r["id"] for r in second_results]
    assert first_ids == second_ids == list(range(10))


def test_uuid_expression_creation():
    """Test that uuid() creates a UUIDExpr."""
    expr = uuid()
    assert isinstance(expr, UUIDExpr)


def test_uuid_expression_structural_equality():
    """Test structural equality comparison for uuid expressions."""
    expr1 = uuid()
    expr2 = uuid()

    # All uuid() expressions should be structurally equal (no parameters)
    assert expr1.structurally_equals(expr2)
    assert expr2.structurally_equals(expr1)


def test_uuid_expression_structural_equality_with_non_uuid_expr():
    """Test structural equality comparison with non-synthetic expression."""
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
