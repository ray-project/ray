import pytest
import numpy as np
from ray.air.checkpoint import Checkpoint
from ray.train.tensorflow.tensorflow_checkpoint import TensorflowCheckpoint
import tensorflow as tf
from ray.train.tests.test_tensorflow_predictor import build_model, DummyPreprocessor


class DummyStatefulPreprocessor(DummyPreprocessor):
    def __init__(self):
        # Generate some arbitrary random state to check that the preprocessor
        # is being correctly loaded
        self._state = tf.random.normal((2, 2))

    def get_state(self):
        return self._state


def test_tensorflow_checkpoint():
    model = build_model()
    # Keras model requires build/call for weights to be initialized
    model.build(input_shape=(1,))
    preprocessor = DummyStatefulPreprocessor()

    checkpoint = TensorflowCheckpoint.from_model(model, preprocessor=preprocessor)

    assert checkpoint.get_model_weights() == model.get_weights()

    with checkpoint.as_directory() as path:
        checkpoint = TensorflowCheckpoint.from_directory(path)
        checkpoint_preprocessor = checkpoint.get_preprocessor()
        assert checkpoint.get_model_weights() == model.get_weights()
        assert np.all(checkpoint_preprocessor.get_state() == preprocessor.get_state())
