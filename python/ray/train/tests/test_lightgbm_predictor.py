import re

import lightgbm as lgbm
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import ray

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MAX_REPR_LENGTH
from ray.air.util.data_batch_conversion import _convert_pandas_to_batch_type
from ray.data.preprocessor import Preprocessor
from ray.train.batch_predictor import BatchPredictor
from ray.train.lightgbm import LightGBMCheckpoint, LightGBMPredictor
from ray.train.predictor import TYPE_TO_ENUM
from typing import Tuple

from ray.train.tests.dummy_preprocessor import DummyPreprocessor


dummy_data = np.array([[1, 2], [3, 4], [5, 6]])
dummy_target = np.array([0, 1, 0])
model = lgbm.LGBMClassifier(n_estimators=10).fit(dummy_data, dummy_target).booster_


def get_num_trees(booster: lgbm.Booster) -> int:
    return booster.current_iteration()


def test_repr():
    predictor = LightGBMPredictor(model=model)

    representation = repr(predictor)

    assert len(representation) < MAX_REPR_LENGTH
    pattern = re.compile("^LightGBMPredictor\\((.*)\\)$")
    assert pattern.match(representation)


def create_checkpoint_preprocessor() -> Tuple[Checkpoint, Preprocessor]:
    preprocessor = DummyPreprocessor()

    checkpoint = LightGBMCheckpoint.from_model(booster=model, preprocessor=preprocessor)

    return checkpoint, preprocessor


def test_lightgbm_checkpoint():
    checkpoint, preprocessor = create_checkpoint_preprocessor()

    predictor = LightGBMPredictor(model=model, preprocessor=preprocessor)

    checkpoint_predictor = LightGBMPredictor.from_checkpoint(checkpoint)

    assert get_num_trees(checkpoint_predictor.model) == get_num_trees(predictor.model)
    assert checkpoint_predictor.get_preprocessor() == predictor.get_preprocessor()


@pytest.mark.parametrize("batch_type", [np.ndarray, pd.DataFrame, dict])
def test_predict(batch_type):
    preprocessor = DummyPreprocessor()
    predictor = LightGBMPredictor(model=model, preprocessor=preprocessor)

    raw_batch = pd.DataFrame([[1, 2], [3, 4], [5, 6]])
    data_batch = _convert_pandas_to_batch_type(raw_batch, type=TYPE_TO_ENUM[batch_type])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert predictor.get_preprocessor().has_preprocessed


@pytest.mark.parametrize("batch_type", [np.ndarray, pd.DataFrame])
def test_predict_batch(ray_start_4_cpus, batch_type):
    checkpoint, _ = create_checkpoint_preprocessor()
    predictor = BatchPredictor.from_checkpoint(checkpoint, LightGBMPredictor)

    raw_batch = pd.DataFrame(dummy_data, columns=["A", "B"])
    data_batch = _convert_pandas_to_batch_type(raw_batch, type=TYPE_TO_ENUM[batch_type])

    if batch_type == np.ndarray:
        # TODO(ekl) how do we fix this to work with "data" column?
        dataset = ray.data.from_numpy(dummy_data)
        dataset = dataset.add_column("__value__", lambda b: b["data"])
        dataset = dataset.drop_columns(["data"])
    elif batch_type == pd.DataFrame:
        dataset = ray.data.from_pandas(data_batch)
    elif batch_type == pa.Table:
        dataset = ray.data.from_arrow(data_batch)
    else:
        raise RuntimeError("Invalid batch_type")

    predictions = predictor.predict(dataset)

    assert predictions.count() == 3


def test_predict_feature_columns():
    preprocessor = DummyPreprocessor()
    predictor = LightGBMPredictor(model=model, preprocessor=preprocessor)

    data_batch = np.array([[1, 2, 7], [3, 4, 8], [5, 6, 9]])
    predictions = predictor.predict(data_batch, feature_columns=[0, 1])

    assert len(predictions) == 3
    assert predictor.get_preprocessor().has_preprocessed


def test_predict_feature_columns_pandas():
    pandas_data = pd.DataFrame(dummy_data, columns=["A", "B"])
    pandas_target = pd.Series(dummy_target)
    pandas_model = (
        lgbm.LGBMClassifier(n_estimators=10).fit(pandas_data, pandas_target).booster_
    )
    preprocessor = DummyPreprocessor()
    predictor = LightGBMPredictor(model=pandas_model, preprocessor=preprocessor)
    data_batch = pd.DataFrame(
        np.array([[1, 2, 7], [3, 4, 8], [5, 6, 9]]), columns=["A", "B", "C"]
    )
    predictions = predictor.predict(data_batch, feature_columns=["A", "B"])

    assert len(predictions) == 3
    assert predictor.get_preprocessor().has_preprocessed


@pytest.mark.parametrize("to_string", [True, False])
def test_predict_feature_columns_pandas_categorical(to_string: bool):
    pandas_data = pd.DataFrame(dummy_data, columns=["A", "B"])
    if to_string:
        pandas_data["A"] = [str(x) for x in pandas_data["A"]]
    pandas_data["A"] = pandas_data["A"].astype("category")
    pandas_target = pd.Series(dummy_target)
    pandas_model = (
        lgbm.LGBMClassifier(n_estimators=10).fit(pandas_data, pandas_target).booster_
    )
    preprocessor = DummyPreprocessor()
    predictor = LightGBMPredictor(model=pandas_model, preprocessor=preprocessor)
    data_batch = pd.DataFrame(
        np.array([[1, 2, 2], [3, 4, 8], [5, 6, 9]]), columns=["A", "B", "C"]
    )
    if to_string:
        data_batch["A"] = [str(x) for x in data_batch["A"]]
    data_batch["A"] = data_batch["A"].astype("category")
    predictions = predictor.predict(data_batch, feature_columns=["A", "B"])

    assert len(predictions) == 3
    assert predictor.get_preprocessor().has_preprocessed


def test_predict_no_preprocessor_no_training():
    checkpoint = LightGBMCheckpoint.from_model(booster=model)
    predictor = LightGBMPredictor.from_checkpoint(checkpoint)

    data_batch = np.array([[1, 2], [3, 4], [5, 6]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
