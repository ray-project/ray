import pytest
import tensorflow as tf
import numpy as np

import ray
from ray.data.preprocessors import Concatenator
from ray.air import session
from ray.train.tensorflow import TensorflowTrainer
from ray.air.config import ScalingConfig
from ray.air.constants import TENSOR_COLUMN_NAME


class TestToTF:
    def test_autosharding_is_disabled(self):
        ds = ray.data.from_items([{"spam": 0, "ham": 0}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham")

        actual_auto_shard_policy = (
            dataset.options().experimental_distribute.auto_shard_policy
        )
        expected_auto_shard_policy = tf.data.experimental.AutoShardPolicy.OFF
        assert actual_auto_shard_policy is expected_auto_shard_policy

    def test_element_spec_type(self):
        ds = ray.data.from_items([{"spam": 0, "ham": 0}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham")

        feature_spec, label_spec = dataset.element_spec
        assert isinstance(feature_spec, tf.TypeSpec)
        assert isinstance(label_spec, tf.TypeSpec)

    def test_element_spec_type_with_multiple_columns(self):
        ds = ray.data.from_items([{"spam": 0, "ham": 0, "eggs": 0}])

        dataset = ds.to_tf(feature_columns=["spam", "ham"], label_columns="eggs")

        feature_output_signature, _ = dataset.element_spec
        assert isinstance(feature_output_signature, dict)
        assert feature_output_signature.keys() == {"spam", "ham"}
        assert all(
            isinstance(value, tf.TypeSpec)
            for value in feature_output_signature.values()
        )

    def test_element_spec_name(self):
        ds = ray.data.from_items([{"spam": 0, "ham": 0}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham")

        feature_spec, label_spec = dataset.element_spec
        assert feature_spec.name == "spam"
        assert label_spec.name == "ham"

    @pytest.mark.parametrize(
        "data, expected_dtype",
        [
            (0, tf.int64),
            (0.0, tf.double),
            (False, tf.bool),
            ("eggs", tf.string),
            (np.zeros([2, 2], dtype=np.float32), tf.float32),
        ],
    )
    def test_element_spec_dtype(self, data, expected_dtype):
        ds = ray.data.from_items([{"spam": data, "ham": data}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham")

        feature_spec, label_spec = dataset.element_spec
        assert feature_spec.dtype == expected_dtype
        assert label_spec.dtype == expected_dtype

    def test_element_spec_shape(self):
        ds = ray.data.from_items(8 * [{"spam": 0, "ham": 0}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham", batch_size=4)

        feature_spec, label_spec = dataset.element_spec
        assert tuple(feature_spec.shape) == (None,)
        assert tuple(label_spec.shape) == (None,)

        features, labels = next(iter(dataset))
        assert tuple(features.shape) == (4,)
        assert tuple(labels.shape) == (4,)

    def test_element_spec_shape_with_tensors(self):
        ds = ray.data.from_items(8 * [{"spam": np.zeros([3, 32, 32]), "ham": 0}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham", batch_size=4)

        feature_spec, _ = dataset.element_spec
        assert tuple(feature_spec.shape) == (None, 3, 32, 32)

        features, labels = next(iter(dataset))
        assert tuple(features.shape) == (4, 3, 32, 32)
        assert tuple(labels.shape) == (4,)

    def test_training(self):
        def build_model() -> tf.keras.Model:
            return tf.keras.Sequential([tf.keras.layers.Dense(1)])

        def train_func():
            strategy = tf.distribute.MultiWorkerMirroredStrategy()
            with strategy.scope():
                multi_worker_model = build_model()
                multi_worker_model.compile(
                    optimizer=tf.keras.optimizers.SGD(),
                    loss=tf.keras.losses.mean_absolute_error,
                    metrics=[tf.keras.metrics.mean_squared_error],
                )

            dataset = session.get_dataset_shard("train").to_tf("X", "Y", batch_size=4)
            multi_worker_model.fit(dataset)

        dataset = ray.data.from_items(8 * [{"X0": 0, "X1": 0, "Y": 0}])
        trainer = TensorflowTrainer(
            train_loop_per_worker=train_func,
            preprocessor=Concatenator(exclude=["Y"], output_column_name="X"),
            scaling_config=ScalingConfig(num_workers=2),
            datasets={"train": dataset},
        )
        trainer.fit()

    def test_invalid_column_raises_error(self):
        ds = ray.data.from_items([{"spam": 0, "ham": 0}])
        with pytest.raises(ValueError):
            ds.to_tf(feature_columns="foo", label_columns="bar")

    def test_simple_dataset_raises_error(self):
        # `range` returns a simple dataset.
        ds = ray.data.range(1)
        with pytest.raises(NotImplementedError):
            ds.to_tf(feature_columns="spam", label_columns="ham")

    def test_tensor_dataset_raises_error(self):
        ds = ray.data.range_tensor(1)
        with pytest.raises(NotImplementedError):
            ds.to_tf(
                feature_columns=TENSOR_COLUMN_NAME, label_columns=TENSOR_COLUMN_NAME
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
