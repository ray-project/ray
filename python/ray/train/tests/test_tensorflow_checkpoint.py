from numpy import ndarray
import os.path
import pytest
import tempfile
import tensorflow as tf
from typing import List
import unittest

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


def get_model():
    return tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=()),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(10),
            tf.keras.layers.Dense(1),
        ]
    )


def test_model_definition_raises_deprecation_warning():
    model = get_model()
    checkpoint = TensorflowCheckpoint.from_model(model)
    with pytest.deprecated_call():
        checkpoint.get_model(model_definition=get_model)


def compare_weights(w1: List[ndarray], w2: List[ndarray]) -> bool:
    if not len(w1) == len(w2):
        return False
    size = len(w1)
    for i in range(size):
        comparison = w1[i] == w2[i]
        if not comparison.all():
            return False

    return True


class TestFromModel(unittest.TestCase):
    def setUp(self):
        self.model = get_model()
        self.preprocessor = DummyPreprocessor(1)

    def test_from_model(self):
        checkpoint = TensorflowCheckpoint.from_model(
            self.model, preprocessor=DummyPreprocessor(1)
        )
        loaded_model = checkpoint.get_model(model=get_model)
        preprocessor = checkpoint.get_preprocessor()

        assert compare_weights(loaded_model.get_weights(), self.model.get_weights())
        assert preprocessor.multiplier == 1

    def test_from_model_no_definition(self):
        checkpoint = TensorflowCheckpoint.from_model(
            self.model, preprocessor=self.preprocessor
        )
        with self.assertRaises(ValueError):
            checkpoint.get_model()

    def test_from_saved_model(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            model_dir_path = os.path.join(tmp_dir, "my_model")
            self.model.save(model_dir_path)
            checkpoint = TensorflowCheckpoint.from_saved_model(
                model_dir_path, preprocessor=DummyPreprocessor(1)
            )
            loaded_model = checkpoint.get_model()
            preprocessor = checkpoint.get_preprocessor()
            assert compare_weights(self.model.get_weights(), loaded_model.get_weights())
            assert preprocessor.multiplier == 1

    def test_from_saved_model_warning_with_model_definition(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            model_dir_path = os.path.join(tmp_dir, "my_model")
            self.model.save(model_dir_path)
            checkpoint = TensorflowCheckpoint.from_saved_model(
                model_dir_path,
                preprocessor=DummyPreprocessor(1),
            )
            with pytest.warns(None):
                loaded_model = checkpoint.get_model(model=get_model)
            preprocessor = checkpoint.get_preprocessor()
            assert compare_weights(self.model.get_weights(), loaded_model.get_weights())
            assert preprocessor.multiplier == 1

    def test_from_h5_model(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            model_file_path = os.path.join(tmp_dir, "my_model.h5")
            self.model.save(model_file_path)
            checkpoint = TensorflowCheckpoint.from_h5(
                model_file_path, preprocessor=DummyPreprocessor(1)
            )
            loaded_model = checkpoint.get_model()
            preprocessor = checkpoint.get_preprocessor()
            assert compare_weights(self.model.get_weights(), loaded_model.get_weights())
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

    sys.exit(pytest.main(["-v", "-x", __file__]))
