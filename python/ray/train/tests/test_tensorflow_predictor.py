import re
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import tensorflow as tf

import ray
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MAX_REPR_LENGTH, MODEL_KEY, PREPROCESSOR_KEY
from ray.air.util.data_batch_conversion import (
    convert_pandas_to_batch_type,
    convert_batch_type_to_pandas,
)
from ray.data.preprocessor import Preprocessor
from ray.train.batch_predictor import BatchPredictor
from ray.train.predictor import TYPE_TO_ENUM
from ray.train.tensorflow import TensorflowCheckpoint, TensorflowPredictor
from typing import Tuple


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class DummyPreprocessor(Preprocessor):
    def transform_batch(self, df):
        self._batch_transformed = True
        return df * 2


def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=()),
            # Add feature dimension, expanding (batch_size,) to (batch_size, 1).
            tf.keras.layers.Flatten(),
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
    model = tf.keras.models.Model(inputs=input, outputs={"a": input, "b": input})
    return model


def build_model_unsupported() -> tf.keras.Model:
    """Builds a model with unsupported output type."""
    input = tf.keras.layers.Input(shape=1)
    model = tf.keras.models.Model(inputs=input, outputs=[input, input])
    return model


weights = [np.array([[2.0]]), np.array([0.0])]


def test_repr():
    predictor = TensorflowPredictor(model_definition=build_model)

    representation = repr(predictor)

    assert len(representation) < MAX_REPR_LENGTH
    pattern = re.compile("^TensorflowPredictor\\((.*)\\)$")
    assert pattern.match(representation)


def create_checkpoint_preprocessor() -> Tuple[Checkpoint, Preprocessor]:
    preprocessor = DummyPreprocessor()
    checkpoint = Checkpoint.from_dict(
        {MODEL_KEY: weights, PREPROCESSOR_KEY: preprocessor}
    )

    return checkpoint, preprocessor


def test_init():
    checkpoint, preprocessor = create_checkpoint_preprocessor()

    predictor = TensorflowPredictor(
        model_definition=build_model, preprocessor=preprocessor, model_weights=weights
    )

    checkpoint_predictor = TensorflowPredictor.from_checkpoint(checkpoint, build_model)

    assert checkpoint_predictor.model_definition == predictor.model_definition
    assert checkpoint_predictor.model_weights == predictor.model_weights
    assert checkpoint_predictor.get_preprocessor() == predictor.get_preprocessor()


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


@pytest.mark.parametrize("batch_type", [np.ndarray, pd.DataFrame, pa.Table, dict])
def test_predict(batch_type):
    predictor = TensorflowPredictor(model_definition=build_model_multi_input)

    raw_batch = pd.DataFrame({"A": [0.0, 0.0, 0.0], "B": [1.0, 2.0, 3.0]})
    data_batch = convert_pandas_to_batch_type(raw_batch, type=TYPE_TO_ENUM[batch_type])
    raw_predictions = predictor.predict(data_batch)
    predictions = convert_batch_type_to_pandas(raw_predictions)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [1.0, 2.0, 3.0]


@pytest.mark.parametrize("batch_type", [pd.DataFrame, pa.Table])
def test_predict_batch(ray_start_4_cpus, batch_type):
    checkpoint = TensorflowCheckpoint.from_dict({MODEL_KEY: {}})
    predictor = BatchPredictor.from_checkpoint(
        checkpoint, TensorflowPredictor, model_definition=build_model_multi_input
    )

    dummy_data = pd.DataFrame([[0.0, 1.0], [0.0, 2.0], [0.0, 3.0]], columns=["A", "B"])

    # Todo: Ray data does not support numpy dicts
    if batch_type == np.ndarray:
        dataset = ray.data.from_numpy(dummy_data.to_numpy())
    elif batch_type == pd.DataFrame:
        dataset = ray.data.from_pandas(dummy_data)
    elif batch_type == pa.Table:
        dataset = ray.data.from_arrow(pa.Table.from_pandas(dummy_data))
    else:
        raise RuntimeError("Invalid batch_type")

    predictions = predictor.predict(dataset)

    assert predictions.count() == 3
    assert predictions.to_pandas().to_numpy().flatten().tolist() == [1.0, 2.0, 3.0]


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


def test_predict_unsupported_output():
    """Tests predictions with models that have unsupported output types."""
    predictor = TensorflowPredictor(model_definition=build_model_unsupported)

    data_batch = np.array([1, 2, 3])
    # Unsupported output should fail
    with pytest.raises(ValueError):
        predictor.predict(data_batch)

    # Using a CustomPredictor should pass.
    class CustomPredictor(TensorflowPredictor):
        def call_model(self, tensor):
            model_output = super().call_model(tensor)
            return {str(i): model_output[i] for i in range(len(model_output))}

    predictor = CustomPredictor(model_definition=build_model_unsupported)
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
    checkpoint = TensorflowCheckpoint.from_model(model)
    batch_predictor = BatchPredictor.from_checkpoint(
        checkpoint, TensorflowPredictor, model_definition=build_model, use_gpu=use_gpu
    )
    predict_dataset = ray.data.range(3)
    predictions = batch_predictor.predict(predict_dataset)
    assert predictions.count() == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
