import sys
import threading
from typing import Dict
from unittest.mock import MagicMock, patch

import numpy as np
import pyarrow as pa
import pytest
import torch

import ray

if sys.version_info <= (3, 12):
    # Skip this test for Python 3.12+ due to to incompatibility tensorflow
    import tensorflow as tf


def build_model():
    import tensorflow as tf

    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=()),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(1),
        ]
    )
    model.compile(optimizer="sgd", loss="mse")
    return model


def test_basic_dataset(ray_start_regular_shared):
    ds = ray.data.range(100)
    it = ds.iterator()
    for _ in range(2):
        result = []
        for batch in it.iter_batches():
            batch = batch["id"]
            result += batch.tolist()
        assert result == list(range(100))

    # TODO(swang): This check currently fails nondeterministically because
    # stats are stored in an actor.
    # https://github.com/ray-project/ray/issues/31571
    # assert it.stats() == ds.stats()


def test_basic_dataset_multi_use_iterator(ray_start_regular_shared):
    """Tests that the iterable outputted by `iter_batches` can be used
    multiple times."""
    ds = ray.data.range(100)
    it = ds.iterator().iter_batches()
    for _ in range(2):
        result = []
        for batch in it:
            batch = batch["id"]
            result += batch.tolist()
        assert result == list(range(100))


def test_basic_dataset_preemption(ray_start_regular_shared):
    """Tests that the iterable outputted by ``iter_batches``
    can be used multiple times even if it is preempted during iteration."""

    ds = ray.data.range(100)
    it = ds.iterator().iter_batches(batch_size=50)
    for _ in range(2):
        result = []
        for i, batch in enumerate(it):
            if i > 0:
                break
            batch = batch["id"]
            result += batch.tolist()

        assert result == list(range(50))


def test_basic_dataset_iter_rows(ray_start_regular_shared):
    ds = ray.data.range(100)
    it = ds.iterator()
    for _ in range(2):
        result = []
        for row in it.iter_rows():
            row = row["id"]
            result.append(row)
        assert result == list(range(100))

    # TODO(swang): This check currently fails nondeterministically because
    # stats are stored in an actor.
    # https://github.com/ray-project/ray/issues/31571
    # assert it.stats() == ds.stats()


@pytest.mark.parametrize("include_additional_columns", [False, True])
def test_tf_conversion(ray_start_regular_shared, include_additional_columns):
    ds = ray.data.range(5)
    it = ds.iterator()

    if include_additional_columns:
        tf_dataset = it.to_tf("id", "id", additional_columns="id")
    else:
        tf_dataset = it.to_tf("id", "id")

    for i, row in enumerate(tf_dataset):
        assert all(row[0] == i)
        assert all(row[1] == i)
        assert isinstance(row[0], tf.Tensor)
        assert isinstance(row[1], tf.Tensor)
        if include_additional_columns:
            assert all(row[2] == i)
            assert isinstance(row[2], tf.Tensor)


@pytest.mark.parametrize("include_additional_columns", [False, True])
def test_tf_e2e(ray_start_regular_shared, include_additional_columns):
    ds = ray.data.range(5)
    it = ds.iterator()
    model = build_model()
    if include_additional_columns:
        model.fit(it.to_tf("id", "id", additional_columns="id"), epochs=3)
    else:
        model.fit(it.to_tf("id", "id"), epochs=3)


def test_torch_conversion(ray_start_regular_shared):
    ds = ray.data.range(5)
    it = ds.iterator()
    it._iter_batches = MagicMock()

    for batch in it.iter_torch_batches():
        assert isinstance(batch["id"], torch.Tensor)
        assert batch["id"].tolist() == list(range(5))

    # When collate_fn is not specified, check that the default
    #  `_collate_fn` (handles formatting and Tensor creation)
    # and `_finalize_fn` (handles host to device data transfer)
    # are used in `DataIterator.iter_batches()`.
    iter_batches_calls_kwargs = [a.kwargs for a in it._iter_batches.call_args_list]
    assert all(
        callable(kwargs["_collate_fn"]) and callable(kwargs["_finalize_fn"])
        for kwargs in iter_batches_calls_kwargs
    ), iter_batches_calls_kwargs


def test_torch_multi_use_iterator(ray_start_regular_shared):
    """Tests that the iterator outputted by `iter_torch_batches` can be used
    multiple times."""
    ds = ray.data.range(5)
    it = ds.iterator().iter_torch_batches()

    for _ in range(2):
        for batch in it:
            assert isinstance(batch["id"], torch.Tensor)
            assert batch["id"].tolist() == list(range(5))


def test_torch_conversion_collate_fn(ray_start_regular_shared):
    def collate_fn(batch: Dict[str, np.ndarray]):
        return torch.as_tensor(batch["id"] + 5)

    ds = ray.data.range(5)
    it = ds.iterator()
    for batch in it.iter_torch_batches(collate_fn=collate_fn):
        assert isinstance(batch, torch.Tensor)
        assert batch.tolist() == list(range(5, 10))

    # Should fail.
    with pytest.raises(ValueError):
        for batch in it.iter_torch_batches(collate_fn=collate_fn, dtypes=torch.float32):
            assert isinstance(batch, torch.Tensor)
            assert batch.tolist() == list(range(5, 10))

    with pytest.raises(ValueError):
        for batch in it.iter_torch_batches(collate_fn=collate_fn, device="cpu"):
            assert isinstance(batch, torch.Tensor)
            assert batch.tolist() == list(range(5, 10))

    # Test that we don't automatically set device if collate_fn is specified.
    with patch(
        "ray.air._internal.torch_utils.get_devices", lambda: [torch.device("cuda")]
    ):
        devices = ray.air._internal.torch_utils.get_devices()
        assert devices[0].type == "cuda"

        it._iter_batches = MagicMock()
        for batch in it.iter_torch_batches(collate_fn=collate_fn):
            assert batch.device.type == "cpu"
            assert isinstance(batch, torch.Tensor)
            assert batch.tolist() == list(range(5, 10))

        # Check that _finalize_fn is always used in `DataIterator.iter_batches()`.
        iter_batches_calls_kwargs = [a.kwargs for a in it._iter_batches.call_args_list]
        assert all(
            kwargs["_finalize_fn"] is not None for kwargs in iter_batches_calls_kwargs
        ), iter_batches_calls_kwargs


@pytest.fixture(params=["regular", "chunked"])
def null_array_table(request):
    """Fixture that returns a PyArrow table with either a regular or chunked null array."""
    if request.param == "regular":
        # Regular array
        return pa.table({"fruit_apple": pa.array([None, None, None], type=pa.null())})
    else:
        # Chunked array
        return pa.table(
            {
                "fruit_apple": pa.chunked_array(
                    [
                        pa.array([None], type=pa.null()),
                        pa.array([None, None], type=pa.null()),
                    ]
                )
            }
        )


def test_torch_conversion_null_type(ray_start_regular_shared, null_array_table):
    """Test iter_torch_batches with a PyArrow table containing null type arrays."""
    ds = ray.data.from_arrow(null_array_table)
    it = ds.iterator()
    for batch in it.iter_torch_batches():
        assert isinstance(batch, dict)
        assert "fruit_apple" in batch
        assert isinstance(batch["fruit_apple"], torch.Tensor)
        assert torch.isnan(batch["fruit_apple"]).all()
        assert batch["fruit_apple"].shape == (3,)


def test_iterator_to_materialized_dataset(ray_start_regular_shared):
    """Tests that `DataIterator.materialize` fully consumes the
    iterator and returns a `MaterializedDataset` view of the data
    that can be used to interact with the full dataset
    (e.g. load it all into memory)."""
    ds = ray.data.range(10)
    num_splits = 2
    iters = ds.streaming_split(num_splits, equal=True)

    def consume_in_parallel(fn):
        runners = [
            threading.Thread(target=fn, args=(it, i)) for i, it in enumerate(iters)
        ]
        [r.start() for r in runners]
        [r.join() for r in runners]

    materialized_ds = {}
    shard_data = {}

    def materialize(it, i):
        materialized_ds[i] = it.materialize()

    def iter_batches(it, i):
        data = []
        for batch in it.iter_batches():
            data.extend(batch["id"].tolist())
        shard_data[i] = data

    consume_in_parallel(materialize)
    consume_in_parallel(iter_batches)

    # Check that the materialized datasets contain the same data as the
    # original iterators.
    for i in range(num_splits):
        assert sorted(materialized_ds[i].to_pandas()["id"].tolist()) == sorted(
            shard_data[i]
        )


if __name__ == "__main__":
    import sys

    if sys.version_info >= (3, 12):
        # Skip this test for Python 3.12+ due to to incompatibility tensorflow
        sys.exit(0)

    sys.exit(pytest.main(["-v", __file__]))
