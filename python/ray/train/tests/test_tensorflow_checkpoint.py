import os.path
import tempfile

import tensorflow as tf

import ray
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import (
    TensorflowCheckpoint,
    TensorflowTrainer,
    TensorflowPredictor,
)
from ray.air import session
from ray.air.config import ScalingConfig
from ray.data import Preprocessor


class DummyPreprocessor(Preprocessor):
    def __init__(self, multiplier):
        self.multiplier = multiplier

    def transform_batch(self, df):
        return df * self.multiplier


def test_saved_model():
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=()),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(10),
            tf.keras.layers.Dense(1),
        ]
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        model_dir_path = os.path.join(tmp_dir, "my_model")
        model.save(model_dir_path)
        checkpoint = TensorflowCheckpoint.from_saved_model(
            model_dir_path, preprocessor=DummyPreprocessor(1)
        )
        loaded_model = checkpoint.get_model()
        preprocessor = checkpoint.get_preprocessor()
        assert model.get_config() == loaded_model.get_config()
        assert preprocessor.multiplier == 1


def test_h5_model():
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=()),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(10),
            tf.keras.layers.Dense(1),
        ]
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        model_file_path = os.path.join(tmp_dir, "my_model.h5")
        model.save(model_file_path)
        checkpoint = TensorflowCheckpoint.from_h5(
            model_file_path, preprocessor=DummyPreprocessor(1)
        )
        loaded_model = checkpoint.get_model()
        preprocessor = checkpoint.get_preprocessor()
        assert model.get_config() == loaded_model.get_config()
        assert preprocessor.multiplier == 1


def test_tensorflow_checkpoint_saved_model():
    # The test passes if the following can run successfully.

    def train_fn():
        model = tf.keras.Sequential(
            [
                tf.keras.layers.InputLayer(input_shape=()),
                tf.keras.layers.Flatten(),
                tf.keras.layers.Dense(10),
                tf.keras.layers.Dense(1),
            ]
        )
        model.save("my_model")
        checkpoint = TensorflowCheckpoint.from_saved_model("my_model")
        session.report({"my_metric": 1}, checkpoint=checkpoint)

    trainer = TensorflowTrainer(
        train_loop_per_worker=train_fn, scaling_config=ScalingConfig(num_workers=2)
    )

    result_checkpoint = trainer.fit().checkpoint

    batch_predictor = BatchPredictor.from_checkpoint(
        result_checkpoint, TensorflowPredictor
    )
    batch_predictor.predict(ray.data.range(3))


def test_tensorflow_checkpoint_h5():
    # The test passes if the following can run successfully.

    def train_func():
        model = tf.keras.Sequential(
            [
                tf.keras.layers.InputLayer(input_shape=()),
                tf.keras.layers.Flatten(),
                tf.keras.layers.Dense(10),
                tf.keras.layers.Dense(1),
            ]
        )
        model.save("my_model.h5")
        checkpoint = TensorflowCheckpoint.from_h5("my_model.h5")
        session.report({"my_metric": 1}, checkpoint=checkpoint)

    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func, scaling_config=ScalingConfig(num_workers=2)
    )

    result_checkpoint = trainer.fit().checkpoint

    batch_predictor = BatchPredictor.from_checkpoint(
        result_checkpoint, TensorflowPredictor
    )
    batch_predictor.predict(ray.data.range(3))


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
