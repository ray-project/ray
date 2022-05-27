from ray.ml.predictors.integrations.tensorflow import TensorflowPredictor
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.constants import PREPROCESSOR_KEY, MODEL_KEY

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
    assert predictions.to_numpy().flatten().tolist() == [2, 4, 6]
    assert hasattr(predictor.preprocessor, "_batch_transformed")


def test_predict_array_with_input_shape_unspecified():
    def model_definition():
        return tf.keras.models.Sequential(tf.keras.layers.Lambda(lambda tensor: tensor))

    predictor = TensorflowPredictor(model_definition=model_definition, model_weights=[])

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [1, 2, 3]


def test_predict_array():
    checkpoint = {MODEL_KEY: weights}
    predictor = TensorflowPredictor.from_checkpoint(
        Checkpoint.from_dict(checkpoint), build_model
    )

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [1, 2, 3]


def test_predict_dataframe_with_feature_columns():
    predictor = TensorflowPredictor(model_definition=build_model, model_weights=weights)

    data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])
    predictions = predictor.predict(data, feature_columns=["A"])

    assert len(predictions) == 2
    assert predictions.to_numpy().flatten().tolist() == [1, 3]


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
