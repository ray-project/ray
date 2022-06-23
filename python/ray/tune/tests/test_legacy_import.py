import warnings

import pytest


@pytest.fixture
def logging_setup():
    warnings.filterwarnings("always")


def test_import_execution_checkpoint_manager(logging_setup):
    with pytest.warns(DeprecationWarning):
        import ray.tune.checkpoint_manager  # noqa: F401


def test_import_execution_cluster_info(logging_setup):
    with pytest.warns(DeprecationWarning):
        import ray.tune.cluster_info  # noqa: F401


def test_import_execution_insufficient_resources_manager(logging_setup):
    with pytest.warns(DeprecationWarning):
        import ray.tune.insufficient_resources_manager  # noqa: F401


def test_import_execution_placement_groups(logging_setup):
    with pytest.warns(DeprecationWarning):
        import ray.tune.utils.placement_groups  # noqa: F401


def test_import_execution_ray_trial_executor(logging_setup):
    with pytest.warns(DeprecationWarning):
        import ray.tune.ray_trial_executor  # noqa: F401


def test_import_execution_trial_runner(logging_setup):
    with pytest.warns(DeprecationWarning):
        import ray.tune.trial_runner  # noqa: F401


def test_import_experiment_config_parser(logging_setup):
    with pytest.warns(DeprecationWarning):
        import ray.tune.config_parser  # noqa: F401


def test_import_experiment_experiment(logging_setup):
    # No warning - original imports still work
    from ray.tune.experiment import Experiment, convert_to_experiment_list  # noqa: F401


def test_import_experiment_trial(logging_setup):
    with pytest.warns(DeprecationWarning):
        import ray.tune.trial  # noqa: F401


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
