import json

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import ray.data
import xgboost as xgb

from ray.air.checkpoint import Checkpoint
from ray.air.util.data_batch_conversion import convert_pandas_to_batch_type
from ray.data.preprocessor import Preprocessor
from ray.train.batch_predictor import BatchPredictor
from ray.train.predictor import TYPE_TO_ENUM
from ray.train.xgboost import XGBoostCheckpoint, XGBoostPredictor
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


dummy_data = np.array([[1, 2], [3, 4], [5, 6]])
dummy_target = np.array([0, 1, 0])
model = xgb.XGBClassifier(n_estimators=10).fit(dummy_data, dummy_target).get_booster()


def get_num_trees(booster: xgb.Booster) -> int:
    data = [json.loads(d) for d in booster.get_dump(dump_format="json")]
    return len(data)


def create_checkpoint_preprocessor() -> Tuple[Checkpoint, Preprocessor]:
    preprocessor = DummyPreprocessor()
    preprocessor.attr = 1

    checkpoint = XGBoostCheckpoint.from_model(booster=model, preprocessor=preprocessor)

    return checkpoint, preprocessor


def test_init():
    checkpoint, preprocessor = create_checkpoint_preprocessor()

    predictor = XGBoostPredictor(model=model, preprocessor=preprocessor)

    checkpoint_predictor = XGBoostPredictor.from_checkpoint(checkpoint)

    assert get_num_trees(checkpoint_predictor.model) == get_num_trees(predictor.model)
    assert (
        checkpoint_predictor.get_preprocessor().attr
        == predictor.get_preprocessor().attr
    )


@pytest.mark.parametrize("batch_type", [np.ndarray, pd.DataFrame, pa.Table, dict])
def test_predict(batch_type):
    preprocessor = DummyPreprocessor()
    predictor = XGBoostPredictor(model=model, preprocessor=preprocessor)

    raw_batch = pd.DataFrame([[1, 2], [3, 4], [5, 6]])
    data_batch = convert_pandas_to_batch_type(raw_batch, type=TYPE_TO_ENUM[batch_type])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert hasattr(predictor.get_preprocessor(), "_batch_transformed")


@pytest.mark.parametrize("batch_type", [np.ndarray, pd.DataFrame, pa.Table])
def test_predict_batch(ray_start_4_cpus, batch_type):
    checkpoint, _ = create_checkpoint_preprocessor()
    predictor = BatchPredictor.from_checkpoint(checkpoint, XGBoostPredictor)

    raw_batch = pd.DataFrame(dummy_data, columns=["A", "B"])
    data_batch = convert_pandas_to_batch_type(raw_batch, type=TYPE_TO_ENUM[batch_type])

    if batch_type == np.ndarray:
        dataset = ray.data.from_numpy(dummy_data)
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
    predictor = XGBoostPredictor(model=model, preprocessor=preprocessor)

    data_batch = np.array([[1, 2, 7], [3, 4, 8], [5, 6, 9]])
    predictions = predictor.predict(data_batch, feature_columns=[0, 1])

    assert len(predictions) == 3
    assert hasattr(predictor.get_preprocessor(), "_batch_transformed")


def test_predict_feature_columns_pandas():
    pandas_data = pd.DataFrame(dummy_data, columns=["A", "B"])
    pandas_target = pd.Series(dummy_target)
    pandas_model = (
        xgb.XGBClassifier(n_estimators=10).fit(pandas_data, pandas_target).get_booster()
    )
    preprocessor = DummyPreprocessor()
    predictor = XGBoostPredictor(model=pandas_model, preprocessor=preprocessor)
    data_batch = pd.DataFrame(
        np.array([[1, 2, 7], [3, 4, 8], [5, 6, 9]]), columns=["A", "B", "C"]
    )
    predictions = predictor.predict(data_batch, feature_columns=["A", "B"])

    assert len(predictions) == 3
    assert hasattr(predictor.get_preprocessor(), "_batch_transformed")


def test_predict_no_preprocessor_no_training():
    checkpoint = XGBoostCheckpoint.from_model(booster=model)
    predictor = XGBoostPredictor.from_checkpoint(checkpoint)

    data_batch = np.array([[1, 2], [3, 4], [5, 6]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
