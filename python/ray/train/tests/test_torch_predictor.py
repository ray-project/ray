import re

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import ray
import torch

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MAX_REPR_LENGTH, MODEL_KEY, PREPROCESSOR_KEY
from ray.air.util.data_batch_conversion import (
    convert_pandas_to_batch_type,
    convert_batch_type_to_pandas,
)
from ray.data.preprocessor import Preprocessor
from ray.train.batch_predictor import BatchPredictor
from ray.train.predictor import TYPE_TO_ENUM
from ray.train.torch import TorchCheckpoint, TorchPredictor


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class DummyPreprocessor(Preprocessor):
    def transform_batch(self, df):
        return df * 2


class DummyModelSingleTensor(torch.nn.Module):
    def forward(self, input):
        return input * 2


class DummyModelMultiInput(torch.nn.Module):
    def forward(self, input_dict):
        return sum(input_dict.values())


class DummyModelMultiOutput(torch.nn.Module):
    def forward(self, input_tensor):
        return {"a": input_tensor, "b": input_tensor}


class DummyCustomModel(torch.nn.Module):
    """A model with an unsupported output type."""

    def forward(self, input_tensor):
        return [input_tensor, input_tensor]


@pytest.fixture
def model():
    return DummyModelSingleTensor()


@pytest.fixture
def preprocessor():
    return DummyPreprocessor()


def test_repr(model):
    predictor = TorchPredictor(model=model)

    representation = repr(predictor)

    assert len(representation) < MAX_REPR_LENGTH
    pattern = re.compile("^TorchPredictor\\((.*)\\)$")
    assert pattern.match(representation)


def test_init(model, preprocessor):
    predictor = TorchPredictor(model=model, preprocessor=preprocessor)

    checkpoint = {MODEL_KEY: model, PREPROCESSOR_KEY: preprocessor}
    checkpoint_predictor = TorchPredictor.from_checkpoint(
        Checkpoint.from_dict(checkpoint)
    )

    assert checkpoint_predictor.model == predictor.model
    assert checkpoint_predictor.get_preprocessor() == predictor.get_preprocessor()


@pytest.mark.parametrize("use_gpu", [False, True])
def test_predict_model_not_training(model, use_gpu):
    predictor = TorchPredictor(model=model, use_gpu=use_gpu)

    data_batch = np.array([1])
    predictor.predict(data_batch)

    assert not predictor.model.training


@pytest.mark.parametrize("batch_type", [np.ndarray, pd.DataFrame, pa.Table, dict])
def test_predict(batch_type):
    predictor = TorchPredictor(model=DummyModelMultiInput())

    raw_batch = pd.DataFrame({"X0": [0.0, 0.0, 0.0], "X1": [1.0, 2.0, 3.0]})
    data_batch = convert_pandas_to_batch_type(raw_batch, type=TYPE_TO_ENUM[batch_type])
    raw_predictions = predictor.predict(data_batch, dtype=torch.float)
    predictions = convert_batch_type_to_pandas(raw_predictions)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [1.0, 2.0, 3.0]


@pytest.mark.parametrize("batch_type", [pd.DataFrame, pa.Table])
def test_predict_batch(ray_start_4_cpus, batch_type):
    checkpoint = TorchCheckpoint.from_dict({MODEL_KEY: {}})
    predictor = BatchPredictor.from_checkpoint(
        checkpoint, TorchPredictor, model=DummyModelMultiInput()
    )

    dummy_data = pd.DataFrame(
        [[0.0, 1.0], [0.0, 2.0], [0.0, 3.0]], columns=["X0", "X1"]
    )

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
def test_predict_array(model, use_gpu):
    predictor = TorchPredictor(model=model, use_gpu=use_gpu)

    data_batch = np.asarray([1, 2, 3])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.flatten().tolist() == [2, 4, 6]


@pytest.mark.parametrize("use_gpu", [False, True])
def test_predict_array_with_preprocessor(model, preprocessor, use_gpu):
    predictor = TorchPredictor(model=model, preprocessor=preprocessor, use_gpu=use_gpu)

    data_batch = np.array([1, 2, 3])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.flatten().tolist() == [4, 8, 12]


@pytest.mark.parametrize("use_gpu", [False, True])
def test_predict_dataframe(use_gpu):
    predictor = TorchPredictor(model=DummyModelMultiInput(), use_gpu=use_gpu)

    data_batch = pd.DataFrame({"X0": [0.0, 0.0, 0.0], "X1": [1.0, 2.0, 3.0]})
    predictions = predictor.predict(data_batch, dtype=torch.float)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [1.0, 2.0, 3.0]


@pytest.mark.parametrize("use_gpu", [False, True])
def test_predict_multi_output(use_gpu):
    predictor = TorchPredictor(model=DummyModelMultiOutput(), use_gpu=use_gpu)

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
    predictor = TorchPredictor(model=DummyCustomModel())

    data_batch = np.array([1, 2, 3])

    # List output is not supported.
    with pytest.raises(ValueError):
        predictor.predict(data_batch)

    # Use a custom predictor instead.
    class CustomPredictor(TorchPredictor):
        def call_model(self, tensor):
            model_output = super().call_model(tensor)
            return {str(i): model_output[i] for i in range(len(model_output))}

    predictor = CustomPredictor(model=DummyCustomModel())
    predictions = predictor.predict(data_batch)
    assert len(predictions) == 2
    for k, v in predictions.items():
        # Each tensor is of size 3
        assert len(v) == 3
        assert v.flatten().tolist() == [1, 2, 3]


@pytest.mark.parametrize("use_gpu", [False, True])
@pytest.mark.parametrize(
    ("input_dtype", "expected_output_dtype"),
    (
        (torch.float16, np.float16),
        (torch.float64, np.float64),
        (torch.int32, np.int32),
        (torch.int64, np.int64),
    ),
)
def test_predict_array_with_different_dtypes(
    model, input_dtype, expected_output_dtype, use_gpu
):
    predictor = TorchPredictor(model=model, use_gpu=use_gpu)

    data_batch = np.array([1, 2, 3])
    predictions = predictor.predict(data_batch, dtype=input_dtype)

    assert predictions.dtype == expected_output_dtype


@pytest.mark.parametrize("use_gpu", [False, True])
def test_predict_array_no_training(model, use_gpu):
    checkpoint = TorchCheckpoint.from_model(model)
    predictor = TorchPredictor.from_checkpoint(checkpoint, use_gpu=use_gpu)

    data_batch = np.array([1, 2, 3])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.flatten().tolist() == [2, 4, 6]


@pytest.mark.parametrize("use_gpu", [False, True])
def test_array_real_model(use_gpu):
    model = torch.nn.Linear(2, 1)
    predictor = TorchPredictor(model=model, use_gpu=use_gpu)

    data = np.array([[1, 2], [3, 4]])
    predictions = predictor.predict(data, dtype=torch.float)
    assert len(predictions) == 2


@pytest.mark.parametrize("use_gpu", [False, True])
def test_multi_modal_real_model(use_gpu):
    class CustomModule(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.linear1 = torch.nn.Linear(1, 1)
            self.linear2 = torch.nn.Linear(1, 1)

        def forward(self, input_dict: dict):
            # Add feature dimension, expanding (batch_size,) to (batch_size, 1).
            input_dict["A"] = input_dict["A"].unsqueeze(1)
            input_dict["B"] = input_dict["B"].unsqueeze(1)
            out1 = self.linear1(input_dict["A"])
            out2 = self.linear2(input_dict["B"])
            return out1 + out2

    predictor = TorchPredictor(model=CustomModule(), use_gpu=use_gpu)

    data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])

    predictions = predictor.predict(data, dtype=torch.float)
    assert len(predictions) == 2
    if use_gpu:
        assert next(
            predictor.model.parameters()
        ).is_cuda, "Model should be moved to GPU if use_gpu is True"
    else:
        assert not next(
            predictor.model.parameters()
        ).is_cuda, "Model should not be on GPU if use_gpu is False"


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
