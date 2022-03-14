import os
from ray.ml.predictors.integrations.lightgbm.lightgbm_predictor import LightGBMPredictor
from ray.ml.predictors.integrations.xgboost import XGBoostPredictor
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.constants import PREPROCESSOR_KEY, MODEL_KEY
import json

import numpy as np
import xgboost as xgb
import lightgbm as lgbm
import tempfile

import pytest


class DummyPreprocessor(Preprocessor):
    def transform_batch(self, df):
        return df * 2


dummy_data = np.array([[1, 2], [3, 4], [5, 6]])
dummy_target = np.array([0, 1, 0])

xgb_model = (
    xgb.XGBClassifier(n_estimators=10).fit(dummy_data, dummy_target).get_booster()
)
lgbm_model = lgbm.LGBMClassifier(n_estimators=10).fit(dummy_data, dummy_target).booster_


def _get_num_trees_xgb(booster: xgb.Booster) -> int:
    data = [json.loads(d) for d in booster.get_dump(dump_format="json")]
    return len(data) // 4


def _get_num_trees_lgbm(booster: lgbm.Booster) -> int:
    return booster.current_iteration()


@pytest.mark.parametrize("model", [xgb_model, lgbm_model])
def test_init(model):
    predictor_cls = (
        XGBoostPredictor if isinstance(model, type(xgb_model)) else LightGBMPredictor
    )
    get_num_trees = (
        _get_num_trees_xgb
        if isinstance(model, type(xgb_model))
        else _get_num_trees_lgbm
    )

    preprocessor = DummyPreprocessor()
    preprocessor.attr = 1
    predictor = predictor_cls(model=model, preprocessor=preprocessor)

    with tempfile.TemporaryDirectory() as tmpdir:
        # This somewhat convoluted procedure is the same as in the
        # Trainers. The reason for saving model to disk instead
        # of directly to the dict as bytes is due to all callbacks
        # following save to disk logic. GBDT models are small
        # enough that IO should not be an issue.
        model.save_model(os.path.join(tmpdir, "model.xgb"))
        checkpoint = Checkpoint.from_dict({PREPROCESSOR_KEY: preprocessor})
        checkpoint.to_directory(path=tmpdir)

        checkpoint = Checkpoint.from_directory(tmpdir)
        checkpoint_predictor = predictor_cls.from_checkpoint(checkpoint)

    assert get_num_trees(checkpoint_predictor.model) == get_num_trees(predictor.model)
    assert checkpoint_predictor.preprocessor.attr == predictor.preprocessor.attr


@pytest.mark.parametrize("model", [xgb_model, lgbm_model])
def test_predict(model):
    predictor_cls = (
        XGBoostPredictor if isinstance(model, type(xgb_model)) else LightGBMPredictor
    )
    preprocessor = DummyPreprocessor()
    predictor = predictor_cls(model=model, preprocessor=preprocessor)

    data_batch = np.array([[1, 2], [3, 4], [5, 6]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3


@pytest.mark.parametrize("model", [xgb_model, lgbm_model])
def test_predict_feature_columns(model):
    predictor_cls = (
        XGBoostPredictor if isinstance(model, type(xgb_model)) else LightGBMPredictor
    )
    preprocessor = DummyPreprocessor()
    predictor = predictor_cls(model=model, preprocessor=preprocessor)

    data_batch = np.array([[1, 2, 7], [3, 4, 8], [5, 6, 9]])
    predictions = predictor.predict(data_batch, feature_columns=[0, 1])

    assert len(predictions) == 3
