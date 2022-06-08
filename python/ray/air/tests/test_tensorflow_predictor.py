import ray
from ray.air.batch_predictor import BatchPredictor
from ray.air.checkpoint import Checkpoint
from ray.air.constants import PREPROCESSOR_KEY, MODEL_KEY
from ray.air.predictors.integrations.tensorflow import (
    TensorflowPredictor,
    to_air_checkpoint,
)
from ray.air.preprocessor import Preprocessor

import numpy as np
import pandas as pd
import tensorflow as tf


class DummyPreprocessor(Preprocessor):
    def transform_batch(self, df):
        self._batch_transformed = True
        return df * 2


def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1,)),
            tf.keras.layers.Dense(1),
        ]
    )
    return model


def build_model_multi_input() -> tf.keras.Model:
    input1 = tf.keras.layers.Input(shape=(1,), name="A")
    input2 = tf.keras.layers.Input(shape=(1,), name="B")
    merged = tf.keras.layers.Concatenate(axis=1)([input1, input2])
    output = tf.keras.layers.Dense(1, input_dim=2)(merged)
    model = tf.keras.models.Model(inputs=[input1, input2], outputs=output)
    return model


weights = [np.array([[1.0]]), np.array([0.0])]


def test_init():
    preprocessor = DummyPreprocessor()
    predictor = TensorflowPredictor(
        model_definition=build_model, preprocessor=preprocessor, model_weights=weights
    )

    checkpoint = {MODEL_KEY: weights, PREPROCESSOR_KEY: preprocessor}
    checkpoint_predictor = TensorflowPredictor.from_checkpoint(
        Checkpoint.from_dict(checkpoint), build_model
    )

    assert checkpoint_predictor.model_definition == predictor.model_definition
    assert checkpoint_predictor.model_weights == predictor.model_weights
    assert checkpoint_predictor.preprocessor == predictor.preprocessor


def test_predict_array_with_preprocessor():
    preprocessor = DummyPreprocessor()
    predictor = TensorflowPredictor(
        model_definition=build_model, preprocessor=preprocessor, model_weights=weights
    )

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.flatten().tolist() == [2, 4, 6]
    assert hasattr(predictor.preprocessor, "_batch_transformed")


def test_predict_array_with_input_shape_unspecified():
    def model_definition():
        return tf.keras.models.Sequential(tf.keras.layers.Lambda(lambda tensor: tensor))

    predictor = TensorflowPredictor(model_definition=model_definition, model_weights=[])

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.flatten().tolist() == [1, 2, 3]


def test_predict_array():
    checkpoint = {MODEL_KEY: weights}
    predictor = TensorflowPredictor.from_checkpoint(
        Checkpoint.from_dict(checkpoint), build_model
    )

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.flatten().tolist() == [1, 2, 3]


def test_predict_dataframe():
    # The multi-input model has a single Dense layer with input size of 2 and output
    # size of 1.
    # Specify the weights as 1 for each input.
    # Specify the bias as 0.
    predictor = TensorflowPredictor(
        model_definition=build_model_multi_input,
        model_weights=[np.array([[1.0], [1.0]]), np.array([0.0])],
    )

    # Input matrix is 2x2
    # Weight matrix is 2x1
    # Output is 2x1
    data = pd.DataFrame({"A": [1, 3], "B": [2, 4]})
    predictions = predictor.predict(data)

    # Input 1 = Column 1 = [1, 3]
    # Input 2 = Column 2 = [2, 4]
    # Dense Layer = [[1, 3], [2, 4]] * [1, 1] = [3, 7]
    assert len(predictions) == 2
    assert predictions.to_numpy().flatten().tolist() == [3, 7]


def test_tensorflow_predictor_no_training():
    model = build_model()
    checkpoint = to_air_checkpoint(model)
    batch_predictor = BatchPredictor.from_checkpoint(
        checkpoint, TensorflowPredictor, model_definition=build_model
    )
    predict_dataset = ray.data.range(3)
    predictions = batch_predictor.predict(predict_dataset)
    assert predictions.count() == 3


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
