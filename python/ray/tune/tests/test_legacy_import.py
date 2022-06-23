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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
