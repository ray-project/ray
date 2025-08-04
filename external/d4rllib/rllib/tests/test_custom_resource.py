import pytest

import ray
from ray import tune
from ray.tune.registry import get_trainable_cls
from ray.tune.result import TRAINING_ITERATION


@pytest.mark.parametrize("algorithm", ["PPO", "IMPALA"])
def test_custom_resource(algorithm):
    if ray.is_initialized:
        ray.shutdown()

    ray.init(
        resources={"custom_resource": 1},
        include_dashboard=False,
    )

    config = (
        get_trainable_cls(algorithm)
        .get_default_config()
        .environment("CartPole-v1")
        .framework("torch")
        .env_runners(
            num_env_runners=1,
            custom_resources_per_env_runner={"custom_resource": 0.01},
        )
        .resources(num_gpus=0)
    )
    stop = {TRAINING_ITERATION: 1}

    tune.Tuner(
        algorithm,
        param_space=config,
        run_config=tune.RunConfig(stop=stop, verbose=0),
        tune_config=tune.TuneConfig(num_samples=1),
    ).fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
