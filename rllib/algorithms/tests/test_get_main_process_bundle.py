import pytest

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.utils import _get_main_process_bundle


def test_get_main_process_bundle():
    # num_learners=0, so main process gets num_cpus_per_learner.
    config = AlgorithmConfig().learners(
        num_learners=0, num_cpus_per_learner=4, num_gpus_per_learner=1
    )
    bundle = _get_main_process_bundle(config)
    assert bundle["CPU"] == 4
    assert bundle["GPU"] == 1

    # custom_resources_for_main_process included in bundle (num_learners>0).
    config = (
        AlgorithmConfig()
        .resources(custom_resources_for_main_process={"my_resource": 1})
        .learners(num_learners=1)
    )
    bundle = _get_main_process_bundle(config)
    assert bundle == {"CPU": 1, "GPU": 0, "my_resource": 1}


def test_default_resource_request_old_stack_custom_resources():
    """custom_resources_for_main_process must appear in the placement group
    even when using the old API stack (enable_rl_module_and_learner=False)."""
    config = (
        AlgorithmConfig()
        .api_stack(
            enable_rl_module_and_learner=False,
            enable_env_runner_and_connector_v2=False,
        )
        .resources(custom_resources_for_main_process={"my_resource": 2})
        .env_runners(num_env_runners=0)
    )
    pg_factory = Algorithm.default_resource_request(config)
    main_bundle = pg_factory.bundles[0]
    assert main_bundle["my_resource"] == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
