import sys
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

pytest.importorskip("jax")


def test_iter_jax_batches(ray_start_regular_shared):
    df1 = pd.DataFrame(
        {"one": [1, 2, 3], "two": [1.0, 2.0, 3.0], "label": [1.0, 2.0, 3.0]}
    )
    df2 = pd.DataFrame(
        {"one": [4, 5, 6], "two": [4.0, 5.0, 6.0], "label": [4.0, 5.0, 6.0]}
    )
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3])
    ds = ray.data.from_pandas([df1, df2, df3])

    iterations = []
    for batch in ds.iter_jax_batches(batch_size=3, paddings=-1):
        iterations.append(
            np.stack((batch["one"], batch["two"], batch["label"]), axis=1)
        )
    combined_iterations = np.concatenate(iterations)
    # 8 rows total, batch_size 3, padding=-1 -> 3 batches of size 3 (9 rows total)
    assert len(combined_iterations) == 9
    expected_values = np.concatenate([df.values, [[-1.0, -1.0, -1.0]]])
    np.testing.assert_array_equal(
        np.sort(combined_iterations, axis=0),
        np.sort(expected_values, axis=0),
    )


def test_iter_jax_batches_with_collate_fn(ray_start_regular_shared):
    from ray.data.collate_fn import NumpyBatchCollateFn

    class CustomCollateFn(NumpyBatchCollateFn):
        def __call__(self, batch):
            # Combine "one" and "two" columns into a single "features" tensor
            return np.stack((batch["one"], batch["two"]), axis=1)

    ds = ray.data.from_items([{"one": i, "two": i + 1} for i in range(10)])
    iterations = []
    for batch in ds.iter_jax_batches(batch_size=2, collate_fn=CustomCollateFn()):
        # The output of collate_fn is now a single numpy array (sharded as jax.Array)
        iterations.append(batch)

    combined_iterations = np.concatenate(iterations)
    # Expected shape: (10, 2)
    expected = np.stack((np.arange(10), np.arange(10) + 1), axis=1)
    np.testing.assert_array_equal(
        np.sort(expected, axis=0),
        np.sort(combined_iterations, axis=0),
    )


def test_iter_jax_batches_with_dtypes(ray_start_regular_shared):
    import jax.numpy as jnp

    ds = ray.data.from_items([{"one": i, "two": i + 0.5} for i in range(10)])

    # Test single dtype
    for batch in ds.iter_jax_batches(batch_size=2, dtypes=jnp.float32):
        assert batch["one"].dtype == jnp.float32
        assert batch["two"].dtype == jnp.float32

    # Test dict of dtypes
    dtypes = {"one": jnp.int32, "two": jnp.float16}
    for batch in ds.iter_jax_batches(batch_size=2, dtypes=dtypes):
        assert batch["one"].dtype == jnp.int32
        assert batch["two"].dtype == jnp.float16

    # Test padding with dtypes
    # ds has 10 rows, batch_size=4 -> 3 batches (4, 4, 2)
    # Without drop_last, 3rd batch is padded to 4.
    for batch in ds.iter_jax_batches(
        batch_size=4, dtypes=jnp.float16, paddings=-1, drop_last=False
    ):
        assert batch["one"].dtype == jnp.float16
        assert batch["two"].dtype == jnp.float16


def test_iter_jax_batches_with_dict_padding(ray_start_regular_shared):

    ds = ray.data.from_items([{"one": i, "two": i + 0.5} for i in range(10)])

    # ds has 10 rows, batch_size=4 -> 3 batches (4, 4, 2)
    # Total 12 rows after padding.
    paddings = {"one": -1, "two": -0.5}
    batches = list(ds.iter_jax_batches(batch_size=4, paddings=paddings))
    assert len(batches) == 3

    combined_one = np.concatenate([batch["one"] for batch in batches])
    combined_two = np.concatenate([batch["two"] for batch in batches])

    assert len(combined_one) == 12
    assert len(combined_two) == 12

    expected_one = np.concatenate([np.arange(10), [-1, -1]])
    expected_two = np.concatenate([np.arange(10) + 0.5, [-0.5, -0.5]])

    np.testing.assert_array_equal(np.sort(combined_one), np.sort(expected_one))
    np.testing.assert_array_equal(np.sort(combined_two), np.sort(expected_two))


def test_iter_jax_batches_batch_size_divisibility_fail(ray_start_regular_shared):
    with patch("jax.local_device_count", return_value=2):
        ds = ray.data.range(10)
        # batch_size must be divisible by num_local_devices=2
        with pytest.raises(
            ValueError,
            match="evenly divisible by the number of local JAX devices",
        ):
            list(ds.iter_jax_batches(batch_size=3))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
