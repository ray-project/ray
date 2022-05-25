import pytest

import numpy as np
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

    assert not predictor.model.training


def test_predict_no_preprocessor(model):
    predictor = TorchPredictor(model=model)

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [2, 4, 6]


def test_predict_with_preprocessor(model, preprocessor):
    predictor = TorchPredictor(model=model, preprocessor=preprocessor)

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [4, 8, 12]


def test_predict_array_output(model):
    """Tests if predictor works if model outputs an array instead of single value."""

    predictor = TorchPredictor(model=model)

    data_batch = np.array([[1, 1], [2, 2], [3, 3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert np.array_equal(
        predictions.to_numpy().flatten().tolist(), [[2, 2], [4, 4], [6, 6]]
    )


def test_predict_feature_columns(model):
    predictor = TorchPredictor(model=model)

    data_batch = np.array([[1, 4], [2, 5], [3, 6]])
    predictions = predictor.predict(data_batch, feature_columns=[0])

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [2, 4, 6]


def test_predict_from_checkpoint_no_preprocessor(model):
    checkpoint = Checkpoint.from_dict({MODEL_KEY: model})
    predictor = TorchPredictor.from_checkpoint(checkpoint)

    data_batch = np.array([[1], [2], [3]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictions.to_numpy().flatten().tolist() == [2, 4, 6]
