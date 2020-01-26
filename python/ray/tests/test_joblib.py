import numpy as np
import joblib
from sklearn.datasets import load_digits
from sklearn.model_selection import RandomizedSearchCV
from sklearn.svm import SVC
from ray.experimental.joblib import register_ray
import ray
import os
import subprocess
from time import time
from joblib import Memory
from sklearn.datasets import fetch_openml
from sklearn.datasets import get_data_home
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.kernel_approximation import Nystroem
from sklearn.kernel_approximation import RBFSampler
from sklearn.metrics import zero_one_loss
from sklearn.pipeline import make_pipeline
from sklearn.svm import LinearSVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.utils import check_array
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier

def test_register_ray():
    register_ray()
    assert 'ray' in joblib.parallel.BACKENDS

def test_ray_backend():
    register_ray()
    from ray.experimental.joblib._raybackend import RayBackend
    with joblib.parallel_backend('ray'):
        assert type(joblib.parallel.get_active_backend()[0])==RayBackend

def test_svm_crossvalidation():
    digits = load_digits()

    param_space = {
        'C': np.logspace(-6, 6, 30),
        'gamma': np.logspace(-8, 8, 30),
        'tol': np.logspace(-4, -1, 30),
        'class_weight': [None, 'balanced'],
    }
    
    model = SVC(kernel='rbf')
    search = RandomizedSearchCV(model, param_space, cv=5, n_iter=100, verbose=10)
    register_ray()

    def test_local():
        if ray.is_initialized():
            ray.shutdown()  
        with joblib.parallel_backend('ray'):
            search.fit(digits.data, digits.target)
        assert ray.is_initialized() 

    def test_multiple_nodes():
        if ray.is_initialized():
            ray.shutdown()
        subprocess.check_output(["ray", "start", "--head", "--num-cpus={}".format(4)])
        ray.init(address="auto")
        with joblib.parallel_backend('ray'):
            search.fit(digits.data, digits.target)
        assert ray.is_initialized()
        subprocess.check_output(["ray", "stop"])
    
    test_local()
    test_multiple_nodes()
memory = Memory(os.path.join(get_data_home(), 'mnist_benchmark_data'),
                mmap_mode='r')
@memory.cache
def load_data(dtype=np.float32, order='F'):
    """Load the data, then cache and memmap the train/test split"""
    ######################################################################
    # Load dataset
    print("Loading dataset...")
    data = fetch_openml('mnist_784')
    X = check_array(data['data'], dtype=dtype, order=order)
    y = data["target"]

    # Normalize features
    X = X / 255

    # Create train-test split (as [Joachims, 2006])
    print("Creating train-test split...")
    n_train = 6000
    X_train = X[:n_train]
    y_train = y[:n_train]

    return X_train, y_train

ESTIMATORS = {
    'CART': DecisionTreeClassifier(),
    'ExtraTrees': ExtraTreesClassifier(n_estimators=10),
    'RandomForest': RandomForestClassifier(),
    'Nystroem-SVM': make_pipeline(
        Nystroem(gamma=0.015, n_components=1000), LinearSVC(C=1)),
    'SampledRBF-SVM': make_pipeline(
        RBFSampler(gamma=0.015, n_components=1000), LinearSVC(C=1)),
    'LogisticRegression-SAG': LogisticRegression(solver='sag', tol=1e-1,
                                                 C=1e4),
    'LogisticRegression-SAGA': LogisticRegression(solver='saga', tol=1e-1,
                                                  C=1e4),
    'MultilayerPerceptron': MLPClassifier(
        hidden_layer_sizes=(32, 32), max_iter=100, alpha=1e-4,
        solver='sgd', learning_rate_init=0.2, momentum=0.9, verbose=1,
        tol=1e-2, random_state=1),
    'MLP-adam': MLPClassifier(
        hidden_layer_sizes=(32, 32), max_iter=100, alpha=1e-4,
        solver='adam', learning_rate_init=0.001, verbose=1,
        tol=1e-2, random_state=1)
}
def test_sklearn_benchmarks():
    register_ray()
    X_train, y_train = load_data(order="C")
    train_time = {}
    random_seed = 0
    num_jobs = 2 # use all available resources
    if ray.is_initialized():
        ray.shutdown()
    subprocess.check_output(["ray", "start", "--head", "--num-cpus={}".format(20)])
    ray.init(address="auto")
    with joblib.parallel_backend('ray'):
        for name in sorted(ESTIMATORS.keys()):
            print("Training %s ... " % name, end="")
            estimator = ESTIMATORS[name]
            estimator_params = estimator.get_params()
            estimator.set_params(**{p: random_seed
                                    for p in estimator_params
                                    if p.endswith("random_state")})

            if "n_jobs" in estimator_params:
                estimator.set_params(n_jobs=num_jobs)
            time_start = time()
            estimator.fit(X_train, y_train)
            train_time[name] = time() - time_start
            print("training", name, "took", train_time[name], "seconds")
    subprocess.check_output(["ray", "stop"])
