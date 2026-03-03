import os
import pickle
import sys
import time
from unittest import mock

import joblib
import numpy as np
import pytest
from sklearn.datasets import load_digits, load_iris
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier
from sklearn.kernel_approximation import Nystroem, RBFSampler
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import RandomizedSearchCV, cross_val_score
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import make_pipeline
from sklearn.svm import SVC, LinearSVC
from sklearn.tree import DecisionTreeClassifier

import ray
from ray._common.test_utils import wait_for_condition
from ray.util.joblib import register_ray
from ray.util.joblib.ray_backend import RayBackend


def test_register_ray():
    register_ray()
    assert "ray" in joblib.parallel.BACKENDS
    assert not ray.is_initialized()


def test_ray_backend(shutdown_only):
    register_ray()

    with joblib.parallel_backend("ray"):
        assert type(joblib.parallel.get_active_backend()[0]) is RayBackend


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


def test_ray_remote_args(shutdown_only):
    ray.init(num_cpus=4, resources={"custom_resource": 4})
    register_ray()

    assert ray.available_resources().get("custom_resource", 0) == 4

    def check_resource():
        assert ray.available_resources().get("custom_resource", 0) < 4

    with joblib.parallel_backend(
        "ray", ray_remote_args={"resources": {"custom_resource": 1}}
    ):
        joblib.Parallel()(joblib.delayed(check_resource)() for i in range(8))


def test_task_to_actor_assignment(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote(num_cpus=0)
    class Counter:
        def __init__(self):
            self._c = 0

        def inc(self):
            self._c += 1

        def get(self) -> int:
            return self._c

    counter = Counter.remote()

    def worker_func(worker_id):
        launch_time = time.time()

        # Wait for all 4 workers to have started.
        ray.get(counter.inc.remote())
        wait_for_condition(lambda: ray.get(counter.get.remote()) == 4)

        return worker_id, launch_time

    output = []
    num_workers = 4
    register_ray()
    with joblib.parallel_backend("ray", n_jobs=-1):
        output = joblib.Parallel()(
            joblib.delayed(worker_func)(worker_id) for worker_id in range(num_workers)
        )

    worker_ids = set()
    launch_times = []
    for worker_id, launch_time in output:
        worker_ids.add(worker_id)
        launch_times.append(launch_time)

    assert len(worker_ids) == num_workers

    for i in range(num_workers):
        for j in range(i + 1, num_workers):
            assert abs(launch_times[i] - launch_times[j]) < 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
