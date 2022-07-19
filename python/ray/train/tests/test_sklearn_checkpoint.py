import tempfile

import numpy as np
from sklearn.linear_model import LinearRegression

from ray.train.sklearn import SklearnCheckpoint


def test_from_checkpoint_and_get_model():
    estimator = LinearRegression()
    X = np.random.uniform(size=(2, 2))
    y = np.random.uniform(size=(2,))
    estimator.fit(X, y)

    checkpoint = SklearnCheckpoint.from_estimator(estimator)

    assert np.array_equal(checkpoint.get_model().coef_, estimator.coef_)


def test_from_checkpoint_kwargs():
    estimator = LinearRegression()
    checkpoint = SklearnCheckpoint.from_estimator(estimator, epoch=0)
    assert checkpoint.to_dict()["epoch"] == 0
