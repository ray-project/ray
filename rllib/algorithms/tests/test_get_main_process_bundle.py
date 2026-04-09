import pytest

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.utils import _get_main_process_bundle


def test_get_main_process_bundle():
    # num_learners=0, default cpus_per_learner: CPU = max(1, 1) = 1, GPU = 0.
    config = AlgorithmConfig()
    bundle = _get_main_process_bundle(config)
    assert bundle == {"CPU": 1, "GPU": 0}

    # num_learners=0, explicit cpus_per_learner > num_cpus_for_main_process.
    config = AlgorithmConfig().learners(
        num_learners=0, num_cpus_per_learner=4, num_gpus_per_learner=1
    )
    bundle = _get_main_process_bundle(config)
    assert bundle["CPU"] == 4
    assert bundle["GPU"] == 1

    # num_learners=0, num_cpus_for_main_process wins over cpus_per_learner.
    config = (
        AlgorithmConfig()
        .resources(num_cpus_for_main_process=8)
        .learners(num_learners=0, num_cpus_per_learner=2)
    )
    bundle = _get_main_process_bundle(config)
    assert bundle["CPU"] == 8

    # num_learners>0: main process gets num_cpus_for_main_process, GPU=0.
    config = AlgorithmConfig().learners(num_learners=2, num_gpus_per_learner=1)
    bundle = _get_main_process_bundle(config)
    assert bundle == {"CPU": 1, "GPU": 0}

    # num_learners>0, custom num_cpus_for_main_process.
    config = (
        AlgorithmConfig()
        .resources(num_cpus_for_main_process=3)
        .learners(num_learners=1)
    )
    bundle = _get_main_process_bundle(config)
    assert bundle["CPU"] == 3
    assert bundle["GPU"] == 0

    # custom_resources_for_main_process included in bundle (num_learners=0).
    config = AlgorithmConfig().resources(
        custom_resources_for_main_process={"on_demand": 0.001}
    )
    bundle = _get_main_process_bundle(config)
    assert bundle["on_demand"] == 0.001
    assert bundle["CPU"] == 1

    # custom_resources_for_main_process included in bundle (num_learners>0).
    config = (
        AlgorithmConfig()
        .resources(custom_resources_for_main_process={"on_demand": 0.001})
        .learners(num_learners=1)
    )
    bundle = _get_main_process_bundle(config)
    assert bundle == {"CPU": 1, "GPU": 0, "on_demand": 0.001}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
