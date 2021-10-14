import pandas as pd

import requests

from sklearn import svm
from sklearn import datasets

import ray
from ray import serve
from ray.serve.util import SKLearnModel


def test_sklearn_wrapper(serve_instance):
    @serve.deployment(route_prefix="/sklearn")
    class MyIrisModel(SKLearnModel):
        def __init__(self):
            # Train simple IRIS model.
            iris = datasets.load_iris()
            X, y = iris.data, iris.target
            clf = svm.SVC(gamma="scale")
            clf.fit(X, y)

            super().__init__(clf)

    MyIrisModel.deploy()
    handle = MyIrisModel.get_handle()
    test_df = pd.DataFrame([[5.1, 3.5, 1.4, 0.123]])

    assert ray.get(handle.remote(test_df)) == [0]
    assert requests.get(
        "http://localhost:8000/sklearn", json=test_df.to_dict()).json() == [0]


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
