import sys

import numpy as np
import pytest

from ray.data.util.jax_util import jax_sync_generator


def test_jax_sync_generator_empty_batch(ray_start_regular_shared):
    try:
        import jax  # noqa: F401
    except ImportError:
        pytest.skip("JAX not installed")

    def empty_batch_iterable():
        yield {"a": np.array([1, 2, 3])}
        yield {}  # Empty dict batch
        yield {"a": np.array([4, 5, 6])}

    gen = jax_sync_generator(empty_batch_iterable(), drop_last=True)
    results = list(gen)

    assert len(results) == 2
    assert np.array_equal(results[0]["a"], np.array([1, 2, 3]))
    assert np.array_equal(results[1]["a"], np.array([4, 5, 6]))


def test_jax_sync_generator_empty_column(ray_start_regular_shared):
    try:
        import jax  # noqa: F401
    except ImportError:
        pytest.skip("JAX not installed")

    def empty_column_iterable():
        yield {"a": np.array([1, 2, 3])}
        yield {"a": np.array([])}  # Dict with empty column
        yield {"a": np.array([4, 5, 6])}

    gen = jax_sync_generator(empty_column_iterable(), drop_last=True)
    results = list(gen)

    assert len(results) == 2
    assert np.array_equal(results[0]["a"], np.array([1, 2, 3]))
    assert np.array_equal(results[1]["a"], np.array([4, 5, 6]))


def test_jax_sync_generator_no_sync(ray_start_regular_shared):
    try:
        import jax  # noqa: F401
    except ImportError:
        pytest.skip("JAX not installed")

    def batches():
        yield {"a": np.array([1, 2, 3])}
        yield {"a": np.array([4, 5, 6])}

    # Should work fine in single process even with sync=False
    gen = jax_sync_generator(batches(), drop_last=True, synchronize_batches=False)
    results = list(gen)
    assert len(results) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
