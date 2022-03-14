from ray.ml.predictors.integrations.tensorflow import TensorflowPredictor
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.constants import PREPROCESSOR_KEY, MODEL_KEY

import numpy as np
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


def test_predict():
    preprocessor = DummyPreprocessor()
    predictor = TensorflowPredictor(
        model_definition=build_model, preprocessor=preprocessor, model_weights=weights
    )

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().round().tolist() == [2, 4, 6]
    assert hasattr(predictor.preprocessor, "_batch_transformed")


def test_predict_feature_columns():
    preprocessor = DummyPreprocessor()
    predictor = TensorflowPredictor(
        model_definition=build_model, preprocessor=preprocessor, model_weights=weights
    )

    data_batch = np.array([[1, 4], [2, 5], [3, 6]])
    predictions = predictor.predict(data_batch, feature_columns=[0])

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().round().tolist() == [2, 4, 6]
    assert hasattr(predictor.preprocessor, "_batch_transformed")


def test_predict_no_preprocessor():
    checkpoint = {MODEL_KEY: weights}
    predictor = TensorflowPredictor.from_checkpoint(
        Checkpoint.from_dict(checkpoint), build_model
    )

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [1, 2, 3]
