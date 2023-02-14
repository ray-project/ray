import pytest
from typing import Dict

import numpy as np
import tensorflow as tf
import torch

import ray


def test_basic_dataset(ray_start_regular_shared):
    ds = ray.data.range(100)
    it = ds.iterator()
    for _ in range(3):
        result = []
        for batch in it.iter_batches():
            result += batch
        assert result == list(range(100))

    # TODO(swang): This check currently fails nondeterministically because
    # stats are stored in an actor.
    # https://github.com/ray-project/ray/issues/31571
    # assert it.stats() == ds.stats()


def test_basic_dataset_pipeline(ray_start_regular_shared):
    ds = ray.data.range(100).window(bytes_per_window=1).repeat()
    it = ds.iterator()
    for _ in range(3):
        result = []
        for batch in it.iter_batches():
            result += batch
        assert result == list(range(100))

    assert it.stats() == ds.stats()


def test_tf_conversion(ray_start_regular_shared):
    ds = ray.data.range_table(5)
    it = ds.iterator()
    tf_dataset = it.to_tf("value", "value")
    for i, row in enumerate(tf_dataset):
        assert all(row[0] == i)
        assert all(row[1] == i)
        assert isinstance(row[0], tf.Tensor)
        assert isinstance(row[1], tf.Tensor)


def test_torch_conversion(ray_start_regular_shared):
    ds = ray.data.range_table(5)
    it = ds.iterator()
    for batch in it.iter_torch_batches():
        assert isinstance(batch["value"], torch.Tensor)
        assert batch["value"].tolist() == list(range(5))


def test_torch_conversion_collate_fn(ray_start_regular_shared):
    def collate_fn(batch: Dict[str, np.ndarray]):
        return torch.as_tensor(batch["value"] + 5)

    ds = ray.data.range_table(5)
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
