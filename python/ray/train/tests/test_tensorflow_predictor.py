import numpy as np
import pandas as pd
import tensorflow as tf
import pytest

import ray
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.data.preprocessor import Preprocessor
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import TensorflowPredictor, to_air_checkpoint


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
    output = tf.keras.layers.Add()([input1, input2])
    model = tf.keras.models.Model(inputs=[input1, input2], outputs=output)
    return model


def build_model_multi_output() -> tf.keras.Model:
    input = tf.keras.layers.Input(shape=1)
    model = tf.keras.models.Model(inputs=input, outputs=[input, input])
    return model


weights = [np.array([[2.0]]), np.array([0.0])]


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


@pytest.mark.parametrize("use_gpu", [False, True])
def test_predict_array(use_gpu):
    predictor = TensorflowPredictor(
        model_definition=build_model, model_weights=weights, use_gpu=use_gpu
    )

    data_batch = np.asarray([1, 2, 3])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.flatten().tolist() == [2, 4, 6]


@pytest.mark.parametrize("use_gpu", [False, True])
def test_predict_array_with_preprocessor(use_gpu):
    preprocessor = DummyPreprocessor()
    predictor = TensorflowPredictor(
        model_definition=build_model,
        preprocessor=preprocessor,
        model_weights=weights,
        use_gpu=use_gpu,
    )

    data_batch = np.array([1, 2, 3])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.flatten().tolist() == [4, 8, 12]


@pytest.mark.parametrize("use_gpu", [False, True])
def test_predict_dataframe(use_gpu):
    predictor = TensorflowPredictor(
        model_definition=build_model_multi_input, use_gpu=use_gpu
    )

    data_batch = pd.DataFrame({"A": [0.0, 0.0, 0.0], "B": [1.0, 2.0, 3.0]})
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [1.0, 2.0, 3.0]


@pytest.mark.parametrize("use_gpu", [False, True])
def test_predict_multi_output(use_gpu):
    predictor = TensorflowPredictor(
        model_definition=build_model_multi_output, use_gpu=use_gpu
    )

    data_batch = np.array([1, 2, 3])
    predictions = predictor.predict(data_batch)

    # Model outputs two tensors
    assert len(predictions) == 2
    for k, v in predictions.items():
        # Each tensor is of size 3
        assert len(v) == 3
        assert v.flatten().tolist() == [1, 2, 3]


@pytest.mark.parametrize("use_gpu", [False, True])
def test_tensorflow_predictor_no_training(use_gpu):
    model = build_model()
    checkpoint = to_air_checkpoint(model)
    batch_predictor = BatchPredictor.from_checkpoint(
        checkpoint, TensorflowPredictor, model_definition=build_model, use_gpu=use_gpu
    )
    predict_dataset = ray.data.range(3)
    predictions = batch_predictor.predict(predict_dataset)
    assert predictions.count() == 3


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
