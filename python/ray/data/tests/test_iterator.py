import sys
import threading
from typing import Dict
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import torch

import ray
from ray.util.debug import _test_some_code_for_memory_leaks

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


def test_to_torch_missing_feature_column(ray_start_regular_shared):
    ds = ray.data.from_pandas(pd.DataFrame({"a": [1, 2], "b": [3, 4]}))
    it = ds.iterator().to_torch(feature_columns=["z"], batch_size=2)
    with pytest.raises(KeyError) as excinfo:
        next(iter(it))
    message = str(excinfo.value)
    assert "z" in message
    assert "Available columns" in message


def test_to_torch_missing_label_column(ray_start_regular_shared):
    ds = ray.data.from_pandas(pd.DataFrame({"a": [1, 2], "b": [3, 4]}))
    it = ds.iterator().to_torch(label_column="z", batch_size=2)
    with pytest.raises(KeyError) as excinfo:
        next(iter(it))
    message = str(excinfo.value)
    assert "z" in message
    assert "Available columns" in message


def test_to_torch_label_feature_overlap(ray_start_regular_shared):
    ds = ray.data.from_pandas(pd.DataFrame({"a": [1, 2], "y": [3, 4]}))
    it = ds.iterator().to_torch(
        label_column="y", feature_columns=["a", "y"], batch_size=2
    )
    with pytest.raises(KeyError) as excinfo:
        next(iter(it))
    message = str(excinfo.value)
    assert "y" in message
    assert "Available columns" in message


def test_to_torch_mixed_list_concat_error(ray_start_regular_shared):
    ds = ray.data.from_pandas(
        pd.DataFrame(
            {
                "a": [[1, 2], [3, 4], [5, 6]],
                "b": [10, 11, 12],
            }
        )
    )
    it = ds.iterator().to_torch(
        feature_columns={"g0": ["a", "b"]},
        feature_column_dtypes=torch.float32,
        batch_size=2,
        unsqueeze_feature_tensors=True,
    )
    with pytest.raises(ValueError) as excinfo:
        next(iter(it))
    message = str(excinfo.value)
    assert "column" in message
    assert "shape" in message or "dims" in message


def test_to_torch_grouped_nested_list_error(ray_start_regular_shared):
    ds = ray.data.from_pandas(
        pd.DataFrame(
            {
                "a": [[1, 2], [3, 4], [5, 6]],
                "b": [[1], [2, 3], [4]],
                "c": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            }
        )
    )
    it = ds.iterator().to_torch(
        feature_columns=[["a", "b"], ["a", "c"]],
        feature_column_dtypes=[torch.float64, torch.int64],
        batch_size=2,
    )
    with pytest.raises(ValueError) as excinfo:
        next(iter(it))
    message = str(excinfo.value)
    assert "column" in message
    assert (
        "nested" in message
        or "ragged" in message
        or "Failed to convert column b" in message
    )


def test_to_torch_homogeneous_list_success(ray_start_regular_shared):
    ds = ray.data.from_pandas(pd.DataFrame({"a": [[1, 2], [3, 4], [5, 6]]}))
    it = ds.iterator().to_torch(feature_columns=["a"], batch_size=2)
    features, labels = next(iter(it))
    assert labels is None
    assert features.shape == (2, 1, 2)
    assert features.dtype == torch.int64


def test_to_torch_no_memory_leak_regression(ray_start_regular_shared):
    col = np.empty(500, dtype=object)
    col[:] = [np.ones((8, 8), dtype=np.int64) for _ in range(500)]
    ds = ray.data.from_pandas(pd.DataFrame({"a": col}))

    def code():
        for _ in ds.iterator().to_torch(feature_columns=["a"], batch_size=50):
            pass

    suspicious_stats = _test_some_code_for_memory_leaks(
        desc="Testing DataIterator.to_torch for memory leaks.",
        init=None,
        code=code,
        repeats=5,
    )
    # Filter out unrelated Ray Data execution setup (autoscaler/tracing) noise to
    # focus on leaks originating from tensor conversion code.
    relevant_suspects = [
        suspect
        for suspect in suspicious_stats
        if any(
            "python/ray/data/util/torch_utils.py" in frame.filename
            for frame in suspect.traceback
        )
    ]
    assert not relevant_suspects


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
    with patch("ray.train.torch.get_device", lambda: torch.device("cuda")):
        devices = ray.train.torch.get_device()
        assert devices.type == "cuda"

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


@pytest.fixture
def image_dataset(ray_start_regular_shared):
    """Fixture that returns a Ray dataset with image-like columns."""
    num_rows = 10
    num_images = 8
    image_size_flat = 224 * 224 * 3  # Flattened image size
    rows = []
    for _ in range(num_rows):
        row = {}
        for i in range(num_images):
            row[f"image_{i}"] = np.random.randint(
                0, 255, size=image_size_flat, dtype=np.uint8
            )
        rows.append(row)

    table = pa.Table.from_pylist(rows)
    return ray.data.from_arrow([table])


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


@pytest.fixture
def collate_fns_with_and_without_threading():
    """Fixture that provides DefaultCollateFn with and without threading."""
    from ray.data.collate_fn import DefaultCollateFn

    collate_fn_with_threading = DefaultCollateFn(num_workers=4)
    collate_fn_no_threading = DefaultCollateFn(num_workers=0)

    return {
        "with_threading": collate_fn_with_threading,
        "without_threading": collate_fn_no_threading,
    }


def _collect_batches(ds, collate_fn, batch_size=5):
    """Helper function to collect batches from iter_torch_batches."""
    batches = []
    for batch in ds.iterator().iter_torch_batches(
        collate_fn=collate_fn, batch_size=batch_size
    ):
        batches.append(batch)
    return batches


def test_torch_conversion_default_collate_fn_threading(
    ray_start_regular_shared,
    image_dataset,
    collate_fns_with_and_without_threading,
):
    """Test DefaultCollateFn with/without threading produces same results."""
    ds = image_dataset
    collate_fns = collate_fns_with_and_without_threading

    batches_with_threading = _collect_batches(ds, collate_fns["with_threading"])
    batches_no_threading = _collect_batches(ds, collate_fns["without_threading"])

    # Verify results are the same
    assert len(batches_with_threading) == len(batches_no_threading)
    for b1, b2 in zip(batches_with_threading, batches_no_threading):
        assert set(b1.keys()) == set(b2.keys())
        for col in b1.keys():
            assert len(b1[col]) == len(b2[col])
            for t1, t2 in zip(b1[col], b2[col]):
                assert torch.equal(t1, t2)


@pytest.mark.parametrize("should_equalize", [True, False])
def test_iterator_to_materialized_dataset(ray_start_regular_shared, should_equalize):
    """Tests that `DataIterator.materialize` fully consumes the
    iterator and returns a `MaterializedDataset` view of the data
    that can be used to interact with the full dataset
    (e.g. load it all into memory)."""
    ds = ray.data.range(10)
    num_splits = 2
    iters = ds.streaming_split(num_splits, equal=should_equalize)

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

    sys.exit(pytest.main(["-v", __file__]))
