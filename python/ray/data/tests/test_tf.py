import sys

import numpy as np
import pandas as pd
import pytest

import ray
from ray import train
from ray.data.preprocessors import Concatenator
from ray.train import ScalingConfig

if sys.version_info <= (3, 12):
    # Skip this test for Python 3.12+ due to tensorflow incompatibility
    import tensorflow as tf

    # if tf version is > 2.16, errors cannot be imported as functions
    # parse version with packaging
    from packaging import version

    from ray.train.tensorflow import TensorflowTrainer

    if version.parse(tf.__version__) >= version.parse("2.16"):
        mse = tf.keras.losses.MeanSquaredError()
        mae = tf.keras.losses.MeanAbsoluteError()
    else:
        mse = tf.keras.losses.mean_squared_error
        mae = tf.keras.losses.mean_absolute_error


class TestToTF:
    def test_autosharding_is_disabled(self):
        ds = ray.data.from_items([{"spam": 0, "ham": 0}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham")

        actual_auto_shard_policy = (
            dataset.options().experimental_distribute.auto_shard_policy
        )
        expected_auto_shard_policy = tf.data.experimental.AutoShardPolicy.OFF
        assert actual_auto_shard_policy is expected_auto_shard_policy

    @pytest.mark.parametrize("include_additional_columns", [False, True])
    def test_element_spec_type(self, include_additional_columns):
        ds = ray.data.from_items([{"spam": 0, "ham": 0, "weight": 0}])

        if include_additional_columns:
            dataset = ds.to_tf(
                feature_columns="spam", label_columns="ham", additional_columns="weight"
            )
            feature_spec, label_spec, additional_spec = dataset.element_spec
        else:
            dataset = ds.to_tf(feature_columns="spam", label_columns="ham")
            feature_spec, label_spec = dataset.element_spec

        assert isinstance(feature_spec, tf.TypeSpec)
        assert isinstance(label_spec, tf.TypeSpec)
        if include_additional_columns:
            assert isinstance(additional_spec, tf.TypeSpec)

    @pytest.mark.parametrize("include_additional_columns", [False, True])
    def test_element_spec_user_provided(self, include_additional_columns):
        ds = ray.data.from_items([{"spam": 0, "ham": 0, "eggs": 0, "weight": 0}])

        if include_additional_columns:
            dataset1 = ds.to_tf(
                feature_columns=["spam", "ham"],
                label_columns="eggs",
                additional_columns="weight",
            )
            feature_spec, label_spec, additional_spec = dataset1.element_spec
            dataset2 = ds.to_tf(
                feature_columns=["spam", "ham"],
                label_columns="eggs",
                additional_columns="weight",
                feature_type_spec=feature_spec,
                label_type_spec=label_spec,
                additional_type_spec=additional_spec,
            )
            (
                feature_output_spec,
                label_output_spec,
                additional_output_spec,
            ) = dataset2.element_spec
        else:
            dataset1 = ds.to_tf(feature_columns=["spam", "ham"], label_columns="eggs")
            feature_spec, label_spec = dataset1.element_spec
            dataset2 = ds.to_tf(
                feature_columns=["spam", "ham"],
                label_columns="eggs",
                feature_type_spec=feature_spec,
                label_type_spec=label_spec,
            )
            feature_output_spec, label_output_spec = dataset2.element_spec

        assert isinstance(label_output_spec, tf.TypeSpec)
        assert isinstance(feature_output_spec, dict)
        assert feature_output_spec.keys() == {"spam", "ham"}
        assert all(
            isinstance(value, tf.TypeSpec) for value in feature_output_spec.values()
        )
        if include_additional_columns:
            assert isinstance(additional_output_spec, tf.TypeSpec)

    @pytest.mark.parametrize("include_additional_columns", [False, True])
    def test_element_spec_type_with_multiple_columns(self, include_additional_columns):
        ds = ray.data.from_items(
            [{"spam": 0, "ham": 0, "eggs": 0, "weight1": 0, "weight2": 0}]
        )

        if include_additional_columns:
            dataset = ds.to_tf(
                feature_columns=["spam", "ham"],
                label_columns="eggs",
                additional_columns=["weight1", "weight2"],
            )
            (
                feature_output_signature,
                _,
                additional_output_signature,
            ) = dataset.element_spec
        else:
            dataset = ds.to_tf(feature_columns=["spam", "ham"], label_columns="eggs")
            feature_output_signature, _ = dataset.element_spec

        assert isinstance(feature_output_signature, dict)
        assert feature_output_signature.keys() == {"spam", "ham"}
        assert all(
            isinstance(value, tf.TypeSpec)
            for value in feature_output_signature.values()
        )

        if include_additional_columns:
            assert isinstance(additional_output_signature, dict)
            assert additional_output_signature.keys() == {"weight1", "weight2"}
            assert all(
                isinstance(value, tf.TypeSpec)
                for value in additional_output_signature.values()
            )

        df = pd.DataFrame(
            {
                "feature1": [0, 1, 2],
                "feature2": [3, 4, 5],
                "label": [0, 1, 1],
                "weight1": [0, 0.1, 0.2],
                "weight2": [0.3, 0.4, 0.5],
            }
        )
        ds = ray.data.from_pandas(df)

        if include_additional_columns:
            dataset = ds.to_tf(
                feature_columns=["feature1", "feature2"],
                label_columns="label",
                additional_columns=["weight1", "weight2"],
                batch_size=3,
            )
            (
                feature_output_signature,
                _,
                additional_output_signature,
            ) = dataset.element_spec
            assert isinstance(additional_output_signature, dict)
            assert additional_output_signature.keys() == {"weight1", "weight2"}
            assert all(
                isinstance(value, tf.TypeSpec)
                for value in additional_output_signature.values()
            )
        else:
            dataset = ds.to_tf(
                feature_columns=["feature1", "feature2"],
                label_columns="label",
                batch_size=3,
            )
            feature_output_signature, _ = dataset.element_spec

        assert isinstance(feature_output_signature, dict)
        assert feature_output_signature.keys() == {"feature1", "feature2"}
        assert all(
            isinstance(value, tf.TypeSpec)
            for value in feature_output_signature.values()
        )

        if include_additional_columns:
            features, labels, additional_metadata = next(iter(dataset))
            assert (
                additional_metadata["weight1"].numpy() == df["weight1"].values
            ).all()
            assert (
                additional_metadata["weight2"].numpy() == df["weight2"].values
            ).all()
        else:
            features, labels = next(iter(dataset))
        assert (labels.numpy() == df["label"].values).all()
        assert (features["feature1"].numpy() == df["feature1"].values).all()
        assert (features["feature2"].numpy() == df["feature2"].values).all()

    @pytest.mark.parametrize("include_additional_columns", [False, True])
    def test_element_spec_name(self, include_additional_columns):
        ds = ray.data.from_items([{"spam": 0, "ham": 0, "weight": 0}])

        if include_additional_columns:
            dataset = ds.to_tf(
                feature_columns="spam", label_columns="ham", additional_columns="weight"
            )
            feature_spec, label_spec, additional_spec = dataset.element_spec
        else:
            dataset = ds.to_tf(feature_columns="spam", label_columns="ham")
            feature_spec, label_spec = dataset.element_spec

        assert feature_spec.name == "spam"
        assert label_spec.name == "ham"
        if include_additional_columns:
            assert additional_spec.name == "weight"

    @pytest.mark.parametrize(
        "data, expected_dtype",
        # Skip this test for Python 3.12+ due to tensorflow incompatibility
        [
            (0, tf.int64),
            (0.0, tf.double),
            (False, tf.bool),
            ("eggs", tf.string),
            ([1.0, 2.0], tf.float64),
            (np.zeros([2, 2], dtype=np.float32), tf.float32),
        ]
        if sys.version_info <= (3, 12)
        else [],
    )
    @pytest.mark.parametrize("include_additional_columns", [False, True])
    def test_element_spec_dtype(self, data, expected_dtype, include_additional_columns):
        ds = ray.data.from_items([{"spam": data, "ham": data, "weight": data}])

        if include_additional_columns:
            dataset = ds.to_tf(
                feature_columns="spam",
                label_columns="ham",
                additional_columns="weight",
            )
            feature_spec, label_spec, additional_spec = dataset.element_spec
        else:
            dataset = ds.to_tf(feature_columns="spam", label_columns="ham")
            feature_spec, label_spec = dataset.element_spec

        assert feature_spec.dtype == expected_dtype
        assert label_spec.dtype == expected_dtype
        if include_additional_columns:
            assert additional_spec.dtype == expected_dtype

    @pytest.mark.parametrize("include_additional_columns", [False, True])
    def test_element_spec_shape(self, include_additional_columns):
        ds = ray.data.from_items(8 * [{"spam": 0, "ham": 0, "weight": 0}])

        if include_additional_columns:
            dataset = ds.to_tf(
                feature_columns="spam",
                label_columns="ham",
                additional_columns="weight",
                batch_size=4,
            )
            feature_spec, label_spec, additional_spec = dataset.element_spec
            assert tuple(additional_spec.shape) == (None,)
        else:
            dataset = ds.to_tf(
                feature_columns="spam", label_columns="ham", batch_size=4
            )
            feature_spec, label_spec = dataset.element_spec

        assert tuple(feature_spec.shape) == (None,)
        assert tuple(label_spec.shape) == (None,)

        if include_additional_columns:
            features, labels, additional_metadata = next(iter(dataset))
            assert tuple(additional_metadata.shape) == (4,)
        else:
            features, labels = next(iter(dataset))
        assert tuple(features.shape) == (4,)
        assert tuple(labels.shape) == (4,)

    @pytest.mark.parametrize("include_additional_columns", [False, True])
    def test_element_spec_shape_with_tensors(self, include_additional_columns):
        ds = ray.data.from_items(
            8
            * [
                {
                    "spam": np.zeros([3, 32, 32]),
                    "ham": 0,
                    "weight": np.zeros([3, 32, 32]),
                }
            ]
        )

        if include_additional_columns:
            dataset = ds.to_tf(
                feature_columns="spam",
                label_columns="ham",
                additional_columns="weight",
                batch_size=4,
            )
            feature_spec, _, additional_spec = dataset.element_spec
            assert tuple(additional_spec.shape) == (None, 3, 32, 32)
        else:
            dataset = ds.to_tf(
                feature_columns="spam", label_columns="ham", batch_size=4
            )
            feature_spec, _ = dataset.element_spec

        assert tuple(feature_spec.shape) == (None, 3, 32, 32)

        if include_additional_columns:
            features, labels, additional_metadata = next(iter(dataset))
            assert tuple(additional_metadata.shape) == (4, 3, 32, 32)
        else:
            features, labels = next(iter(dataset))
        assert tuple(features.shape) == (4, 3, 32, 32)
        assert tuple(labels.shape) == (4,)

    @pytest.mark.parametrize("batch_size", [1, 2])
    @pytest.mark.parametrize("include_additional_columns", [False, True])
    def test_element_spec_shape_with_ragged_tensors(
        self, batch_size, include_additional_columns
    ):
        df = pd.DataFrame(
            {
                "spam": [np.zeros([32, 32, 3]), np.zeros([64, 64, 3])],
                "ham": [0, 0],
                "weight": [np.zeros([32, 32, 3]), np.zeros([64, 64, 3])],
            }
        )
        ds = ray.data.from_pandas(df)

        if include_additional_columns:
            dataset = ds.to_tf(
                feature_columns="spam",
                label_columns="ham",
                additional_columns="weight",
                batch_size=batch_size,
            )
            feature_spec, _, additional_spec = dataset.element_spec
            assert tuple(additional_spec.shape) == (None, None, None, None)
        else:
            dataset = ds.to_tf(
                feature_columns="spam", label_columns="ham", batch_size=batch_size
            )
            feature_spec, _ = dataset.element_spec

        assert tuple(feature_spec.shape) == (None, None, None, None)

        if include_additional_columns:
            features, labels, additional_metadata = next(iter(dataset))
            assert tuple(additional_metadata.shape) == (batch_size, None, None, None)
        else:
            features, labels = next(iter(dataset))
        assert tuple(features.shape) == (batch_size, None, None, None)
        assert tuple(labels.shape) == (batch_size,)

    @pytest.mark.parametrize("include_additional_columns", [False, True])
    def test_training(self, include_additional_columns):
        def build_model() -> tf.keras.Model:
            return tf.keras.Sequential([tf.keras.layers.Dense(1)])

        def train_func():
            strategy = tf.distribute.MultiWorkerMirroredStrategy()
            with strategy.scope():
                multi_worker_model = build_model()
                multi_worker_model.compile(
                    optimizer=tf.keras.optimizers.SGD(),
                    loss=mae,
                    metrics=[mse],
                )

            if include_additional_columns:
                dataset = train.get_dataset_shard("train").to_tf(
                    "X", "Y", additional_columns="W", batch_size=4
                )
            else:
                dataset = train.get_dataset_shard("train").to_tf("X", "Y", batch_size=4)
            multi_worker_model.fit(dataset)

        dataset = ray.data.from_items(8 * [{"X0": 0, "X1": 0, "Y": 0, "W": 0}])
        concatenator = Concatenator(columns=["X0", "X1"], output_column_name="X")
        dataset = concatenator.transform(dataset)

        trainer = TensorflowTrainer(
            train_loop_per_worker=train_func,
            scaling_config=ScalingConfig(num_workers=2),
            datasets={"train": dataset},
        )
        trainer.fit()

    @pytest.mark.parametrize("include_additional_columns", [False, True])
    def test_invalid_column_raises_error(self, include_additional_columns):
        ds = ray.data.from_items([{"spam": 0, "ham": 0, "weight": 0}])
        with pytest.raises(ValueError):
            if include_additional_columns:
                ds.to_tf(
                    feature_columns="spam",
                    label_columns="ham",
                    additional_columns="baz",
                )
            else:
                ds.to_tf(feature_columns="foo", label_columns="bar")


if __name__ == "__main__":
    import sys

    if sys.version_info >= (3, 12):
        # Skip this test for Python 3.12+ due to to incompatibility tensorflow
        sys.exit(0)

    sys.exit(pytest.main(["-v", __file__]))
