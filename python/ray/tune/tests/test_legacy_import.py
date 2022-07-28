import subprocess
import sys
import warnings

import pytest


@pytest.fixture
def logging_setup():
    warnings.filterwarnings("always")


@pytest.mark.parametrize(
    "module",
    [
        "ray.tune.checkpoint_manager",
        "ray.tune.cluster_info",
        "ray.tune.insufficient_resources_manager",
        "ray.tune.utils.placement_groups",
        "ray.tune.ray_trial_executor",
        "ray.tune.trial_runner",
        "ray.tune.config_parser",
        "ray.tune.trial",
        "ray.tune.suggest.ax",
        "ray.tune.suggest.bayesopt",
        "ray.tune.suggest.bohb",
        "ray.tune.suggest.dragonfly",
        "ray.tune.suggest.flaml",
        "ray.tune.suggest.hebo",
        "ray.tune.suggest.hyperopt",
        "ray.tune.suggest.nevergrad",
        "ray.tune.suggest.optuna",
        "ray.tune.suggest.sigopt",
        "ray.tune.suggest.skopt",
        "ray.tune.suggest.zoopt",
        "ray.tune.suggest.basic_variant",
        "ray.tune.suggest.search",
        "ray.tune.suggest.search_generator",
        "ray.tune.suggest.suggestion",
        "ray.tune.suggest.util",
        "ray.tune.suggest.variant_generator",
        "ray.tune.sample",
        "ray.tune.function_runner",
        "ray.tune.session",
    ],
)
def test_import_module_raises_warnings(module):
    py_cmd = (
        f"import pytest\n"
        f"with pytest.warns(DeprecationWarning):\n"
        f"      import {module}\n"
        f"\n"
    )
    # We need to run this in a separate process to make sure we don't have
    # leftover state from previous runs
    subprocess.check_call([sys.executable, "-c", py_cmd])


def test_import_experiment_experiment(logging_setup):
    # No warning - original imports still work
    from ray.tune.experiment import (  # noqa: F401
        Experiment,  # noqa: F401
        _convert_to_experiment_list,  # noqa: F401
    )  # noqa: F401


def test_import_logger_all(logging_setup):
    # No warning - original imports still work
    import ray.tune.logger  # noqa: F401


@pytest.mark.parametrize(
    "top_level",
    [
        "ray.tune",
        "ray.tune.analysis",
        "ray.tune.automl",
        "ray.tune.cli",
        "ray.tune.execution",
        "ray.tune.experiment",
        "ray.tune.impl",
        "ray.tune.integration",
        "ray.tune.logger",
        "ray.tune.schedulers",
        "ray.tune.search",
        "ray.tune.stopper",
        "ray.tune.trainable",
        "ray.tune.utils",
    ],
)
def test_import_wildcard(top_level):
    py_cmd = f"from {top_level} import *\n"
    subprocess.check_call([sys.executable, "-c", py_cmd])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
