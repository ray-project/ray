import joblib
import sys
import time
import os
import pytest
from unittest import mock

import pickle
import numpy as np

from sklearn.datasets import load_digits, load_iris
from sklearn.model_selection import RandomizedSearchCV
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.kernel_approximation import Nystroem
from sklearn.kernel_approximation import RBFSampler
from sklearn.pipeline import make_pipeline
from sklearn.svm import LinearSVC, SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import cross_val_score

from ray.util.joblib import register_ray
import ray


def test_register_ray():
    register_ray()
    assert "ray" in joblib.parallel.BACKENDS
    assert not ray.is_initialized()


def test_ray_backend(shutdown_only):
    register_ray()
    from ray.util.joblib.ray_backend import RayBackend

    with joblib.parallel_backend("ray"):
        assert type(joblib.parallel.get_active_backend()[0]) == RayBackend


def test_svm_single_node(shutdown_only):
    digits = load_digits()
    param_space = {
        "C": np.logspace(-6, 6, 10),
        "gamma": np.logspace(-8, 8, 10),
        "tol": np.logspace(-4, -1, 3),
        "class_weight": [None, "balanced"],
    }

    class MockParallel(joblib.Parallel):
        def _terminate_backend(self):
            if self._backend is not None:
                # test ObjectRef caching (PR #16879)
                assert any(o is digits.data for o, ref in self._backend._pool._registry)
                self._backend.terminate()

    model = SVC(kernel="rbf")
    with mock.patch("sklearn.model_selection._search.Parallel", MockParallel):
        search = RandomizedSearchCV(model, param_space, cv=3, n_iter=2, verbose=10)
        register_ray()
        with joblib.parallel_backend("ray"):
            search.fit(digits.data, digits.target)
    assert ray.is_initialized()


def test_svm_multiple_nodes(ray_start_cluster_2_nodes):
    digits = load_digits()
    param_space = {
        "C": np.logspace(-6, 6, 30),
        "gamma": np.logspace(-8, 8, 30),
        "tol": np.logspace(-4, -1, 30),
        "class_weight": [None, "balanced"],
    }

    class MockParallel(joblib.Parallel):
        def _terminate_backend(self):
            if self._backend is not None:
                # test ObjectRef caching (PR #16879)
                assert any(o is digits.data for o, ref in self._backend._pool._registry)
                self._backend.terminate()

    model = SVC(kernel="rbf")
    with mock.patch("sklearn.model_selection._search.Parallel", MockParallel):
        search = RandomizedSearchCV(model, param_space, cv=5, n_iter=2, verbose=10)
        register_ray()
        with joblib.parallel_backend("ray"):
            search.fit(digits.data, digits.target)
    assert ray.is_initialized()


"""This test only makes sure the different sklearn classifiers are supported
and do not fail. It can be improved to check for accuracy similar to
'test_cross_validation' but the classifiers need to be improved (to improve
the accuracy), which results in longer test time.
"""


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_sklearn_benchmarks(ray_start_cluster_2_nodes):
    ESTIMATORS = {
        "CART": DecisionTreeClassifier(),
        "ExtraTrees": ExtraTreesClassifier(n_estimators=10),
        "RandomForest": RandomForestClassifier(),
        "Nystroem-SVM": make_pipeline(
            Nystroem(gamma=0.015, n_components=1000), LinearSVC(C=1)
        ),
        "SampledRBF-SVM": make_pipeline(
            RBFSampler(gamma=0.015, n_components=1000), LinearSVC(C=1)
        ),
        "LogisticRegression-SAG": LogisticRegression(solver="sag", tol=1e-1, C=1e4),
        "LogisticRegression-SAGA": LogisticRegression(solver="saga", tol=1e-1, C=1e4),
        "MultilayerPerceptron": MLPClassifier(
            hidden_layer_sizes=(32, 32),
            max_iter=100,
            alpha=1e-4,
            solver="sgd",
            learning_rate_init=0.2,
            momentum=0.9,
            verbose=1,
            tol=1e-2,
            random_state=1,
        ),
        "MLP-adam": MLPClassifier(
            hidden_layer_sizes=(32, 32),
            max_iter=100,
            alpha=1e-4,
            solver="adam",
            learning_rate_init=0.001,
            verbose=1,
            tol=1e-2,
            random_state=1,
        ),
    }
    # Load dataset.
    print("Loading dataset...")
    unnormalized_X_train, y_train = pickle.load(
        open(os.path.join(os.path.dirname(__file__), "mnist_784_100_samples.pkl"), "rb")
    )
    # Normalize features.
    X_train = unnormalized_X_train / 255

    register_ray()
    train_time = {}
    random_seed = 0
    # Use two workers per classifier.
    num_jobs = 2
    with joblib.parallel_backend("ray"):
        for name in sorted(ESTIMATORS.keys()):
            print("Training %s ... " % name, end="")
            estimator = ESTIMATORS[name]
            estimator_params = estimator.get_params()
            estimator.set_params(
                **{
                    p: random_seed
                    for p in estimator_params
                    if p.endswith("random_state")
                }
            )

            if "n_jobs" in estimator_params:
                estimator.set_params(n_jobs=num_jobs)
            time_start = time.time()
            estimator.fit(X_train, y_train)
            train_time[name] = time.time() - time_start
            print("training", name, "took", train_time[name], "seconds")


def test_cross_validation(shutdown_only):
    register_ray()
    iris = load_iris()
    clf = SVC(kernel="linear", C=1, random_state=0)
    with joblib.parallel_backend("ray", n_jobs=5):
        accuracy = cross_val_score(clf, iris.data, iris.target, cv=5)
    assert len(accuracy) == 5
    for result in accuracy:
        assert result > 0.95


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
