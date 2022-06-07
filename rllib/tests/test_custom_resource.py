import pytest

import ray
from ray import tune


@pytest.mark.parametrize("algorithm", ["PPO", "APEX", "IMPALA"])
def test_custom_resource(algorithm):
    if ray.is_initialized:
        ray.shutdown()

    ray.init(
        resources={"custom_resource": 1},
        include_dashboard=False,
    )

    config = {
        "env": "CartPole-v0",
        "num_workers": 1,
        "num_gpus": 0,
        "framework": "torch",
        "custom_resources_per_worker": {"custom_resource": 0.01},
    }

    stop = {"training_iteration": 1}

    tune.run(
        algorithm,
        config=config,
        stop=stop,
        num_samples=1,
        verbose=0,
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
