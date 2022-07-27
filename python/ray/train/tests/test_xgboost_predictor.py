import json
import os
import tempfile

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import xgboost as xgb

from ray.air._internal.checkpointing import save_preprocessor_to_dir
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY
from ray.air.util.data_batch_conversion import convert_pandas_to_batch_type
from ray.data.preprocessor import Preprocessor
from ray.train.predictor import TYPE_TO_ENUM
from ray.train.xgboost import XGBoostCheckpoint, XGBoostPredictor


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


def test_init():
    preprocessor = DummyPreprocessor()
    preprocessor.attr = 1
    predictor = XGBoostPredictor(model=model, preprocessor=preprocessor)

    with tempfile.TemporaryDirectory() as tmpdir:
        # This somewhat convoluted procedure is the same as in the
        # Trainers. The reason for saving model to disk instead
        # of directly to the dict as bytes is due to all callbacks
        # following save to disk logic. GBDT models are small
        # enough that IO should not be an issue.
        model.save_model(os.path.join(tmpdir, MODEL_KEY))
        save_preprocessor_to_dir(preprocessor, tmpdir)

        checkpoint = Checkpoint.from_directory(tmpdir)
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
