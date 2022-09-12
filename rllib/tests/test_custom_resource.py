import pytest

import ray
from ray import air
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

    if algorithm == "APEX":
        config["num_steps_sampled_before_learning_starts"] = 0

    stop = {"training_iteration": 1}

    tune.Tuner(
        algorithm,
        param_space=config,
        run_config=air.RunConfig(stop=stop, verbose=0),
        tune_config=tune.TuneConfig(num_samples=1),
    ).fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
