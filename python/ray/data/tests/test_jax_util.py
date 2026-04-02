import sys
from unittest.mock import MagicMock, patch

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

    gen = jax_sync_generator(empty_batch_iterable(), drop_last=True, batch_size=3)
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

    gen = jax_sync_generator(empty_column_iterable(), drop_last=True, batch_size=3)
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
    gen = jax_sync_generator(
        batches(), drop_last=True, batch_size=3, synchronize_batches=False
    )
    results = list(gen)
    assert len(results) == 2


def test_jax_sync_generator_padding(ray_start_regular_shared):
    try:
        import jax  # noqa: F401
    except ImportError:
        pytest.skip("JAX not installed")

    def batches():
        yield {"a": np.array([1, 2, 3])}
        yield {"a": np.array([4, 5])}

    # Should pad the second batch to size 3 with value -1
    gen = jax_sync_generator(
        batches(),
        drop_last=False,
        batch_size=3,
        pad_token_ids=-1,
        synchronize_batches=False,
    )
    results = list(gen)

    assert len(results) == 2
    assert len(results[0]["a"]) == 3
    assert len(results[1]["a"]) == 3
    assert np.array_equal(results[1]["a"], np.array([4, 5, -1]))


def test_jax_sync_generator_drop_last(ray_start_regular_shared):
    try:
        import jax  # noqa: F401
    except ImportError:
        pytest.skip("JAX not installed")

    def batches():
        yield {"a": np.array([1, 2, 3])}
        yield {"a": np.array([4, 5])}

    # Should drop the second batch because it's not size 3 and pad_token_id=None
    # Note: in single host, it doesn't drop unless we use _iter_batches(drop_last=True)
    # But jax_sync_generator with drop_last=True will raise error if sizes don't match min.
    # Actually, jax_sync_generator logic for single host just passes through if not sync.

    # Let's test single host with divisibility check failure
    gen = jax_sync_generator(
        batches(),
        drop_last=False,
        batch_size=3,
        pad_token_ids=None,
        synchronize_batches=False,
    )
    results = list(gen)
    assert len(results) == 2  # Both yielded because 2 is divisible by 1 local device

    # Let's force num_local_devices = 4 for testing error handling
    from unittest.mock import patch

    with patch("jax.local_device_count", return_value=4):
        gen = jax_sync_generator(
            batches(),
            drop_last=True,
            batch_size=3,
            pad_token_ids=None,
            synchronize_batches=False,
        )
        with pytest.raises(
            ValueError,
            match="evenly divisible by the number of local JAX devices",
        ):
            list(gen)


def test_jax_sync_generator_multi_host_uneven_batches_with_padding(
    ray_start_regular_shared,
):
    try:
        import jax  # noqa: F401
    except ImportError:
        pytest.skip("JAX not installed")

    from unittest.mock import patch

    def batches():
        yield {"a": np.array([1, 2, 3])}
        # Host 0 ends here, Host 1 has more

    # Mock jax environment: 2 hosts, 1 device per host
    with patch("jax.process_count", return_value=2), patch(
        "jax.local_device_count", return_value=1
    ), patch(
        "ray.data.util.jax_util._convert_batch",
        side_effect=lambda x, sharding, **kwargs: x,
    ):

        def mock_process_allgather(arr):
            # Simulate Host 1 having more data
            # local_infos for host 0: [1, 3, 0, 0, ...] (from batches())
            # arr is a JAX array because jax_sync_generator does jnp.array(local_infos)
            # Convert to numpy for easy manipulation
            h0_infos = np.array(arr)

            h1_infos = np.zeros_like(h0_infos)
            h1_infos[0] = 1  # batch 1 exists
            h1_infos[1] = 3  # batch 1 size
            h1_infos[2] = 1  # batch 2 exists
            h1_infos[3] = 3  # batch 2 size

            import jax.numpy as jnp

            return jnp.array([h0_infos, h1_infos])

        with patch(
            "jax.experimental.multihost_utils.process_allgather",
            side_effect=mock_process_allgather,
        ):
            # Host 0 uses jax_sync_generator
            gen = jax_sync_generator(
                batches(),
                drop_last=False,
                batch_size=3,
                pad_token_ids=-1,
                synchronize_batches=True,
            )
            # Should yield 2 batches: one real, one dummy
            results = list(gen)
            assert len(results) == 2
            assert np.array_equal(results[0]["a"], np.array([1, 2, 3]))
            assert np.array_equal(results[1]["a"], np.array([-1, -1, -1]))


def test_jax_sync_generator_multi_host_uneven_batches_drop_last(
    ray_start_regular_shared,
):
    try:
        import jax  # noqa: F401
    except ImportError:
        pytest.skip("JAX not installed")

    from unittest.mock import patch

    def batches():
        yield {"a": np.array([1, 2, 3])}
        yield {"a": np.array([4, 5, 6])}

    with patch("jax.process_count", return_value=2), patch(
        "jax.local_device_count", return_value=1
    ), patch(
        "ray.data.util.jax_util._convert_batch",
        side_effect=lambda x, sharding, **kwargs: x,
    ):

        def mock_process_allgather(arr):
            # Host 0 has 2 batches, Host 1 has 1 batch
            h0_infos = np.array(arr)
            h1_infos = np.zeros_like(h0_infos)
            h1_infos[0] = 1
            h1_infos[1] = 3
            import jax.numpy as jnp

            return jnp.array([h0_infos, h1_infos])

        with patch(
            "jax.experimental.multihost_utils.process_allgather",
            side_effect=mock_process_allgather,
        ):
            gen = jax_sync_generator(
                batches(),
                drop_last=True,
                batch_size=3,
                synchronize_batches=True,
            )
            # Should yield only 1 batch and stop
            results = list(gen)
            assert len(results) == 1
            assert np.array_equal(results[0]["a"], np.array([1, 2, 3]))


def test_jax_sync_generator_multi_host_uneven_batch_sizes_fail(
    ray_start_regular_shared,
):
    try:
        import jax  # noqa: F401
    except ImportError:
        pytest.skip("JAX not installed")

    from unittest.mock import patch

    def batches():
        yield {"a": np.array([1, 2, 3])}

    with patch("jax.process_count", return_value=2), patch(
        "jax.local_device_count", return_value=1
    ), patch(
        "ray.data.util.jax_util._convert_batch",
        side_effect=lambda x, sharding, **kwargs: x,
    ):

        def mock_process_allgather(arr):
            # Host 0 batch size 3, Host 1 batch size 2
            h0_infos = np.array(arr)
            h1_infos = h0_infos.copy()
            h1_infos[1] = 2
            import jax.numpy as jnp

            return jnp.array([h0_infos, h1_infos])

        with patch(
            "jax.experimental.multihost_utils.process_allgather",
            side_effect=mock_process_allgather,
        ):
            gen = jax_sync_generator(
                batches(),
                drop_last=False,
                batch_size=3,
                synchronize_batches=True,
            )
            with pytest.raises(
                ValueError, match="Uneven batch sizes detected across JAX workers"
            ):
                list(gen)


def test_jax_sync_generator_multi_host_uneven_num_batches_fail(
    ray_start_regular_shared,
):
    try:
        import jax  # noqa: F401
    except ImportError:
        pytest.skip("JAX not installed")

    from unittest.mock import patch

    def batches():
        yield {"a": np.array([1, 2, 3])}
        yield {"a": np.array([4, 5, 6])}

    with patch("jax.process_count", return_value=2), patch(
        "jax.local_device_count", return_value=1
    ), patch(
        "ray.data.util.jax_util._convert_batch",
        side_effect=lambda x, sharding, **kwargs: x,
    ):

        def mock_process_allgather(arr):
            # Host 0 has 2 batches, Host 1 has 1 batch, no padding
            h0_infos = np.array(arr)
            h1_infos = np.zeros_like(h0_infos)
            h1_infos[0] = 1
            h1_infos[1] = 3
            import jax.numpy as jnp

            return jnp.array([h0_infos, h1_infos])

        with patch(
            "jax.experimental.multihost_utils.process_allgather",
            side_effect=mock_process_allgather,
        ):
            gen = jax_sync_generator(
                batches(),
                drop_last=False,
                batch_size=3,
                synchronize_batches=True,
            )
            with pytest.raises(
                ValueError,
                match="Uneven number of batches detected across JAX workers",
            ):
                list(gen)


def test_jax_sync_generator_with_dtypes(ray_start_regular_shared):
    try:
        import jax  # noqa: F401
    except ImportError:
        pytest.skip("JAX not installed")

    def batches():
        yield {"a": np.array([1, 2, 3])}

    import jax.numpy as jnp

    dtypes = {"a": jnp.float16}

    # Mock _convert_batch to capture dtypes
    mock_convert = MagicMock(side_effect=lambda x, sharding, dtypes=None: x)

    with patch("ray.data.util.jax_util._convert_batch", mock_convert):
        gen = jax_sync_generator(
            batches(),
            drop_last=False,
            batch_size=3,
            dtypes=dtypes,
            synchronize_batches=False,
        )
        list(gen)

    # Verify that dtypes was passed to _convert_batch
    mock_convert.assert_called_once()
    assert mock_convert.call_args[1]["dtypes"] == dtypes


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
