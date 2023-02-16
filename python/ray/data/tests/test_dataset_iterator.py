import pytest

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


def test_tf_e2e(ray_start_regular_shared):
    ds = ray.data.range_table(5)
    it = ds.iterator()
    model = build_model()
    model.fit(it.to_tf("value", "value"), epochs=3)


def test_tf_e2e_pipeline(ray_start_regular_shared):
    ds = ray.data.range_table(5).repeat(2)
    it = ds.iterator()
    model = build_model()
    model.fit(it.to_tf("value", "value"), epochs=2)

    ds = ray.data.range_table(5).repeat(2)
    it = ds.iterator()
    model = build_model()
    # 3 epochs fails since we only repeated twice.
    with pytest.raises(Exception, match=r"generator raised StopIteration"):
        model.fit(it.to_tf("value", "value"), epochs=3)


def test_tf_conversion_pipeline(ray_start_regular_shared):
    ds = ray.data.range_table(5).repeat(2)
    it = ds.iterator()
    tf_dataset = it.to_tf("value", "value")
    for i, row in enumerate(tf_dataset):
        assert all(row[0] == i)
        assert all(row[1] == i)
        assert isinstance(row[0], tf.Tensor)
        assert isinstance(row[1], tf.Tensor)

    # Repeated twice.
    tf_dataset = it.to_tf("value", "value")
    for i, row in enumerate(tf_dataset):
        assert all(row[0] == i)
        assert all(row[1] == i)
        assert isinstance(row[0], tf.Tensor)
        assert isinstance(row[1], tf.Tensor)

    # Fails on third try.
    with pytest.raises(Exception, match=r"generator raised StopIteration"):
        tf_dataset = it.to_tf("value", "value")
        for _ in tf_dataset:
            pass


def test_torch_conversion(ray_start_regular_shared):
    ds = ray.data.range_table(5)
    it = ds.iterator()
    for batch in it.iter_torch_batches():
        assert isinstance(batch["value"], torch.Tensor)
        assert batch["value"].tolist() == list(range(5))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
