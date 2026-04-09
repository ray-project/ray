import pytest

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
