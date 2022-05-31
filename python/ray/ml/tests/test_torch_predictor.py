import pytest

import numpy as np
import pandas as pd
import torch

from ray.ml.predictors.integrations.torch import TorchPredictor
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.constants import PREPROCESSOR_KEY, MODEL_KEY


class DummyPreprocessor(Preprocessor):
    def transform_batch(self, df):
        return df * 2


class DummyModel(torch.nn.Linear):
    def forward(self, input):
        return input * 2


@pytest.fixture
def model():
    return DummyModel(1, 1)


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

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [2, 4, 6]


def test_predict_array_with_preprocessor(model, preprocessor):
    predictor = TorchPredictor(model=model, preprocessor=preprocessor)

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [4, 8, 12]


def test_predict_dataframe():
    predictor = TorchPredictor(model=torch.nn.Linear(2, 1, bias=False))

    data_batch = pd.DataFrame({"X0": [0.0, 0.0, 0.0], "X1": [0.0, 0.0, 0.0]})
    predictions = predictor.predict(data_batch, dtype=torch.float)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [0.0, 0.0, 0.0]


@pytest.mark.parametrize(
    ("input_dtype", "expected_output_dtype"),
    (
        (torch.float16, np.float16),
        (torch.float64, np.float64),
        (torch.int32, np.int32),
        (torch.int64, np.int64),
    ),
)
def test_predict_array_with_different_dtypes(input_dtype, expected_output_dtype):
    predictor = TorchPredictor(model=torch.nn.Identity())

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch, dtype=input_dtype)

    assert all(
        prediction.dtype == expected_output_dtype
        for prediction in predictions["predictions"]
    )


def test_predict_dataframe_with_feature_columns():
    predictor = TorchPredictor(model=torch.nn.Identity())

    data_batch = pd.DataFrame({"X0": [0.0, 0.0, 0.0], "X1": [1.0, 1.0, 1.0]})
    predictions = predictor.predict(data_batch, feature_columns=["X0"])

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [0.0, 0.0, 0.0]


def test_predict_array_from_checkpoint(model):
    checkpoint = Checkpoint.from_dict({MODEL_KEY: model})
    predictor = TorchPredictor.from_checkpoint(checkpoint)

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [2, 4, 6]


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
