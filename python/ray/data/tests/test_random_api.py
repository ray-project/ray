"""
This file tests the reproducibility of the random API with different seed configurations.
"""

import pytest

import ray
from ray.data import RandomSeedConfig
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

DATASET_SIZE = 40
NUM_BLOCKS = 8


@pytest.fixture
def base_dataset(ray_start_regular_shared) -> ray.data.Dataset:
    """Create a base dataset for testing."""
    ctx = DataContext.get_current()
    ctx.enable_progress_bars = False
    return ray.data.range(DATASET_SIZE, override_num_blocks=NUM_BLOCKS)


def create_seed_config(seed_param):
    """Create seed config from parametrized values."""
    if seed_param is None:
        return None
    if isinstance(seed_param, int):
        return seed_param
    if seed_param == "RandomSeedConfig_false":
        return RandomSeedConfig(seed=42, reseed_after_execution=False)
    if seed_param == "RandomSeedConfig_true":
        return RandomSeedConfig(seed=42, reseed_after_execution=True)
    raise ValueError(f"Unknown seed_param: {seed_param}")


def collect_batches_from_epochs(ds, num_epochs=3, batch_size=5):
    """Collect batches from multiple epochs of iter_batches."""
    epochs = []
    for _ in range(num_epochs):
        epoch_data = []
        for batch in ds.iter_batches(batch_size=batch_size, batch_format="pandas"):
            epoch_data.extend(batch["id"].tolist())
        epochs.append(epoch_data)
    return epochs


def apply_random_operation(ds, method, seed, fraction=None):
    """Apply the specified random operation to the dataset."""
    if method == "random_shuffle":
        return ds.random_shuffle(seed=seed)
    if method == "random_sample":
        return ds.random_sample(fraction=fraction or 0.5, seed=seed)
    if method == "randomize_block_order":
        return ds.randomize_block_order(seed=seed)
    raise ValueError(f"Unknown method: {method}")


def test_random_api_deterministic_seed(base_dataset):
    """Test that integer seed produces deterministic results."""
    seed = 42

    for method in ["random_shuffle", "random_sample", "randomize_block_order"]:
        ds1 = apply_random_operation(base_dataset, method, seed)
        ds2 = apply_random_operation(base_dataset, method, seed)
        result1 = ds1.take_all()
        result2 = ds2.take_all()

        if method == "random_sample":
            result1 = sorted(result1, key=lambda x: x["id"])
            result2 = sorted(result2, key=lambda x: x["id"])

        assert result1 == result2, f"Same seed should produce same results for {method}"


@pytest.mark.parametrize(
    "method", ["random_shuffle", "random_sample", "randomize_block_order"]
)
def test_random_api_different_seeds(base_dataset, method):
    """Test that different seeds produce different results."""
    seed1, seed2 = 42, 123

    ds1 = apply_random_operation(base_dataset, method, seed1)
    ds2 = apply_random_operation(base_dataset, method, seed2)
    result1 = ds1.take_all()
    result2 = ds2.take_all()

    if method == "random_sample":
        result1 = sorted(result1, key=lambda x: x["id"])
        result2 = sorted(result2, key=lambda x: x["id"])

    assert (
        result1 != result2
    ), f"Different seeds should produce different results for {method}"


@pytest.mark.parametrize(
    "method", ["random_shuffle", "random_sample", "randomize_block_order"]
)
def test_random_api_none_seed(base_dataset, method):
    """Test that seed=None produces non-deterministic results."""

    ds1 = apply_random_operation(base_dataset, method, None)
    ds2 = apply_random_operation(base_dataset, method, None)
    result1 = ds1.take_all()
    result2 = ds2.take_all()

    if method == "random_sample":
        result1 = sorted(result1, key=lambda x: x["id"])
        result2 = sorted(result2, key=lambda x: x["id"])

    # For randomize_block_order, the shuffle only happens if num_blocks > 1.
    assert (
        result1 != result2
    ), f"seed=None should produce non-deterministic results for {method}"


def test_random_api_reseed_behavior(base_dataset):
    """Test reseed_after_execution behavior across multiple epochs."""
    seed_value = 42
    seed_false = RandomSeedConfig(seed=seed_value, reseed_after_execution=False)
    seed_true = RandomSeedConfig(seed=seed_value, reseed_after_execution=True)

    # Test random_shuffle with reseed_after_execution=False
    ds = base_dataset.random_shuffle(seed=seed_false)
    epochs = collect_batches_from_epochs(ds, num_epochs=3, batch_size=5)
    assert (
        epochs[0] == epochs[1] == epochs[2]
    ), "reseed_after_execution=False should produce same results"

    # Test random_shuffle with reseed_after_execution=True
    ds = base_dataset.random_shuffle(seed=seed_true)
    epochs = collect_batches_from_epochs(ds, num_epochs=3, batch_size=5)
    assert (epochs[0] != epochs[1]) or (
        epochs[1] != epochs[2]
    ), "reseed_after_execution=True should produce different results per epoch"

    # Test random_sample with reseed_after_execution=False
    ds = base_dataset.random_sample(fraction=0.5, seed=seed_false)
    epochs = collect_batches_from_epochs(ds, num_epochs=2, batch_size=5)
    assert sorted(epochs[0]) == sorted(
        epochs[1]
    ), "reseed_after_execution=False should produce same sampled items"

    # Test randomize_block_order with reseed_after_execution=False
    ds = base_dataset.randomize_block_order(seed=seed_false)
    epochs = collect_batches_from_epochs(ds, num_epochs=2, batch_size=5)
    assert (
        epochs[0] == epochs[1]
    ), "reseed_after_execution=False should produce same block order"


@pytest.mark.parametrize(
    "method", ["random_shuffle", "random_sample", "randomize_block_order"]
)
@pytest.mark.parametrize(
    "seed_param", [None, 42, "RandomSeedConfig_false", "RandomSeedConfig_true"]
)
def test_random_api_multiple_epochs(base_dataset, method, seed_param):
    """Comprehensive test for all methods and seed configurations across multiple epochs."""
    seed = create_seed_config(seed_param)
    ds = apply_random_operation(
        base_dataset, method, seed, fraction=0.5 if method == "random_sample" else None
    )
    epochs = collect_batches_from_epochs(ds, num_epochs=3, batch_size=5)

    # Verify epoch data
    assert len(epochs) == 3
    for i, epoch in enumerate(epochs):
        if method == "random_sample":
            assert len(epoch) > 0, f"Epoch {i} should have data"
        else:
            assert (
                len(epoch) == DATASET_SIZE
            ), f"Epoch {i} should have all {DATASET_SIZE} items"

    # Test seed behavior
    if seed_param is None:
        # seed=None should produce non-deterministic results
        # iter_batches() may trigger re-execution, causing different results per epoch
        all_same = all(epoch == epochs[0] for epoch in epochs)
        assert (
            not all_same
        ), f"seed=None should produce different results across epochs for {method}"

    elif seed_param == 42 or seed_param == "RandomSeedConfig_false":
        # Fixed seed or reseed_after_execution=False should produce same results
        assert (
            epochs[0] == epochs[1] == epochs[2]
        ), f"Fixed seed should produce same results across epochs for {method}"

    elif seed_param == "RandomSeedConfig_true":
        # reseed_after_execution=True should produce different results per epoch
        # iter_batches() may trigger re-execution with different execution_idx
        # Check all epochs are pairwise different by converting to tuples and using set
        epoch_tuples = {tuple(epoch) for epoch in epochs}
        assert len(epoch_tuples) == len(
            epochs
        ), f"reseed_after_execution=True should produce different results per epoch for {method}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
