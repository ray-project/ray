import numpy as np
import pandas as pd
import pytest
import torch

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.data.preprocessor import Preprocessor
from ray.train.torch import TorchPredictor, to_air_checkpoint


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
        return [input_tensor, input_tensor]


@pytest.fixture
def model():
    return DummyModelSingleTensor()


@pytest.fixture
def preprocessor():
    return DummyPreprocessor()


def test_init(model, preprocessor):
    predictor = TorchPredictor(model=model, preprocessor=preprocessor)

    checkpoint = {MODEL_KEY: model, PREPROCESSOR_KEY: preprocessor}
    checkpoint_predictor = TorchPredictor.from_checkpoint(
        Checkpoint.from_dict(checkpoint)
    )

    assert checkpoint_predictor.model == predictor.model
    assert checkpoint_predictor.preprocessor == predictor.preprocessor


def test_predict_model_not_training(model):
    predictor = TorchPredictor(model=model)

    data_batch = np.array([1])
    predictor.predict(data_batch)

    assert not predictor.model.training


def test_predict_array(model):
    predictor = TorchPredictor(model=model)

    data_batch = np.asarray([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.flatten().tolist() == [2, 4, 6]


def test_predict_array_with_preprocessor(model, preprocessor):
    predictor = TorchPredictor(model=model, preprocessor=preprocessor)

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.flatten().tolist() == [4, 8, 12]


def test_predict_dataframe():
    predictor = TorchPredictor(model=DummyModelMultiInput())

    data_batch = pd.DataFrame({"X0": [0.0, 0.0, 0.0], "X1": [1.0, 2.0, 3.0]})
    predictions = predictor.predict(data_batch, dtype=torch.float)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [1.0, 2.0, 3.0]


def test_predict_multi_output():
    predictor = TorchPredictor(model=DummyModelMultiOutput())

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    # Model outputs two tensors
    assert len(predictions) == 2
    for k, v in predictions.items():
        # Each tensor is of size 3
        assert len(v) == 3
        assert v.flatten().tolist() == [1, 2, 3]


@pytest.mark.parametrize(
    ("input_dtype", "expected_output_dtype"),
    (
        (torch.float16, np.float16),
        (torch.float64, np.float64),
        (torch.int32, np.int32),
        (torch.int64, np.int64),
    ),
)
def test_predict_array_with_different_dtypes(model, input_dtype, expected_output_dtype):
    predictor = TorchPredictor(model=model)

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch, dtype=input_dtype)

    assert predictions.dtype == expected_output_dtype


def test_predict_array_no_training(model):
    checkpoint = to_air_checkpoint(model)
    predictor = TorchPredictor.from_checkpoint(checkpoint)

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.flatten().tolist() == [2, 4, 6]


def test_array_real_model():
    model = torch.nn.Linear(2, 1)
    predictor = TorchPredictor(model=model)

    data = np.array([[1, 2], [3, 4]])
    predictions = predictor.predict(data, dtype=torch.float)
    assert len(predictions) == 2


def test_multi_modal_real_model():
    class CustomModule(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.linear1 = torch.nn.Linear(1, 1)
            self.linear2 = torch.nn.Linear(1, 1)

        def forward(self, input_dict: dict):
            out1 = self.linear1(input_dict["A"])
            out2 = self.linear2(input_dict["B"])
            return out1 + out2

    predictor = TorchPredictor(model=CustomModule())

    data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])

    predictions = predictor.predict(data, dtype=torch.float)
    assert len(predictions) == 2


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
