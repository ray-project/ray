import pytest
from typing import Dict
from unittest.mock import patch

import numpy as np
import tensorflow as tf
import torch

import ray


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


def test_basic_dataset_pipeline(ray_start_regular_shared):
    ds = ray.data.range(100).window(bytes_per_window=1).repeat()
    it = ds.iterator()
    for _ in range(2):
        result = []
        for batch in it.iter_batches():
            batch = batch["id"].tolist()
            result += batch
        assert result == list(range(100))

    assert it.stats() == ds.stats()


def test_basic_dataset_pipeline_iter_rows(ray_start_regular_shared):
    ds = ray.data.range(100).window(bytes_per_window=1).repeat()
    it = ds.iterator()
    for _ in range(2):
        result = []
        for row in it.iter_rows():
            row = row["id"]
            result.append(row)
        assert result == list(range(100))

    assert it.stats() == ds.stats()


def test_tf_conversion(ray_start_regular_shared):
    ds = ray.data.range(5)
    it = ds.iterator()
    tf_dataset = it.to_tf("id", "id")
    for i, row in enumerate(tf_dataset):
        assert all(row[0] == i)
        assert all(row[1] == i)
        assert isinstance(row[0], tf.Tensor)
        assert isinstance(row[1], tf.Tensor)


def test_tf_e2e(ray_start_regular_shared):
    ds = ray.data.range(5)
    it = ds.iterator()
    model = build_model()
    model.fit(it.to_tf("id", "id"), epochs=3)


def test_tf_e2e_pipeline(ray_start_regular_shared):
    ds = ray.data.range(5).repeat(2)
    it = ds.iterator()
    model = build_model()
    model.fit(it.to_tf("id", "id"), epochs=2)

    ds = ray.data.range(5).repeat(2)
    it = ds.iterator()
    model = build_model()
    # 3 epochs fails since we only repeated twice.
    with pytest.raises(Exception, match=r"generator raised StopIteration"):
        model.fit(it.to_tf("id", "id"), epochs=3)


def test_tf_conversion_pipeline(ray_start_regular_shared):
    ds = ray.data.range(5).repeat(2)
    it = ds.iterator()
    tf_dataset = it.to_tf("id", "id")
    for i, row in enumerate(tf_dataset):
        assert all(row[0] == i)
        assert all(row[1] == i)
        assert isinstance(row[0], tf.Tensor)
        assert isinstance(row[1], tf.Tensor)

    # Repeated twice.
    tf_dataset = it.to_tf("id", "id")
    for i, row in enumerate(tf_dataset):
        assert all(row[0] == i)
        assert all(row[1] == i)
        assert isinstance(row[0], tf.Tensor)
        assert isinstance(row[1], tf.Tensor)

    # Fails on third try.
    with pytest.raises(Exception, match=r"generator raised StopIteration"):
        tf_dataset = it.to_tf("id", "id")
        for _ in tf_dataset:
            pass


def test_torch_conversion(ray_start_regular_shared):
    ds = ray.data.range(5)
    it = ds.iterator()
    for batch in it.iter_torch_batches():
        assert isinstance(batch["id"], torch.Tensor)
        assert batch["id"].tolist() == list(range(5))


def test_torch_conversion_pipeline(ray_start_regular_shared):
    ds = ray.data.range(5).repeat(2)
    it = ds.iterator()

    # First epoch.
    for batch in it.iter_torch_batches():
        assert isinstance(batch["id"], torch.Tensor)
        assert batch["id"].tolist() == list(range(5))

    # Second epoch.
    for batch in it.iter_torch_batches():
        assert isinstance(batch["id"], torch.Tensor)
        assert batch["id"].tolist() == list(range(5))

    # Fails on third iteration.
    with pytest.raises(Exception, match=r"generator raised StopIteration"):
        for batch in it.iter_torch_batches():
            pass


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
        "ray.air._internal.torch_utils.get_device", lambda: torch.device("cuda")
    ):
        assert ray.air._internal.torch_utils.get_device().type == "cuda"
        for batch in it.iter_torch_batches(collate_fn=collate_fn):
            assert batch.device.type == "cpu"
            assert isinstance(batch, torch.Tensor)
            assert batch.tolist() == list(range(5, 10))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
