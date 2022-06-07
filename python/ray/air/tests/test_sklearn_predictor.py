import os
import tempfile
import pytest

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

import ray
import ray.cloudpickle as cpickle
from ray.air.predictors.integrations.sklearn import SklearnPredictor
from ray.air.preprocessor import Preprocessor
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY
from ray.air.batch_predictor import BatchPredictor
from ray.air._internal.checkpointing import save_preprocessor_to_dir


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
model = RandomForestClassifier(n_estimators=10, random_state=0).fit(
    dummy_data, dummy_target
)


def test_init():
    preprocessor = DummyPreprocessor()
    preprocessor.attr = 1
    predictor = SklearnPredictor(estimator=model, preprocessor=preprocessor)

    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, MODEL_KEY), "wb") as f:
            cpickle.dump(model, f)
        save_preprocessor_to_dir(preprocessor, tmpdir)

        checkpoint = Checkpoint.from_directory(tmpdir)
        checkpoint_predictor = SklearnPredictor.from_checkpoint(checkpoint)

    assert np.allclose(
        checkpoint_predictor.estimator.feature_importances_,
        predictor.estimator.feature_importances_,
    )
    assert checkpoint_predictor.preprocessor.attr == predictor.preprocessor.attr


def test_predict():
    preprocessor = DummyPreprocessor()
    predictor = SklearnPredictor(estimator=model, preprocessor=preprocessor)

    data_batch = np.array([[1, 2], [3, 4], [5, 6]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3
    assert hasattr(predictor.preprocessor, "_batch_transformed")


def test_predict_set_cpus(ray_start_4_cpus):
    preprocessor = DummyPreprocessor()
    predictor = SklearnPredictor(estimator=model, preprocessor=preprocessor)

    data_batch = np.array([[1, 2], [3, 4], [5, 6]])
    predictions = predictor.predict(data_batch, num_estimator_cpus=2)

    assert len(predictions) == 3
    assert hasattr(predictor.preprocessor, "_batch_transformed")
    assert predictor.estimator.n_jobs == 2


def test_predict_feature_columns():
    preprocessor = DummyPreprocessor()
    predictor = SklearnPredictor(estimator=model, preprocessor=preprocessor)

    data_batch = np.array([[1, 2, 7], [3, 4, 8], [5, 6, 9]])
    predictions = predictor.predict(data_batch, feature_columns=[0, 1])

    assert len(predictions) == 3
    assert hasattr(predictor.preprocessor, "_batch_transformed")


def test_predict_feature_columns_pandas():
    pandas_data = pd.DataFrame(dummy_data, columns=["A", "B"])
    pandas_target = pd.Series(dummy_target)
    pandas_model = RandomForestClassifier(n_estimators=10, random_state=0).fit(
        pandas_data, pandas_target
    )
    preprocessor = DummyPreprocessor()
    predictor = SklearnPredictor(estimator=pandas_model, preprocessor=preprocessor)
    data_batch = pd.DataFrame(
        np.array([[1, 2, 7], [3, 4, 8], [5, 6, 9]]), columns=["A", "B", "C"]
    )
    predictions = predictor.predict(data_batch, feature_columns=["A", "B"])

    assert len(predictions) == 3
    assert hasattr(predictor.preprocessor, "_batch_transformed")


def test_predict_no_preprocessor():
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, MODEL_KEY), "wb") as f:
            cpickle.dump(model, f)

        checkpoint = Checkpoint.from_directory(tmpdir)
        predictor = SklearnPredictor.from_checkpoint(checkpoint)

    data_batch = np.array([[1, 2], [3, 4], [5, 6]])
    predictions = predictor.predict(data_batch)

    assert len(predictions) == 3


def test_batch_prediction_with_set_cpus(ray_start_4_cpus):
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, MODEL_KEY), "wb") as f:
            cpickle.dump(model, f)

        checkpoint = Checkpoint.from_directory(tmpdir)

        batch_predictor = BatchPredictor.from_checkpoint(checkpoint, SklearnPredictor)

        test_dataset = ray.data.from_pandas(
            pd.DataFrame(dummy_data, columns=["A", "B"])
        )
        batch_predictor.predict(
            test_dataset, num_cpus_per_worker=2, num_estimator_cpus=2
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
