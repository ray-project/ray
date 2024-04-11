import pytest

import ray
from ray import air
from ray import tune
from ray.tune.registry import get_trainable_cls


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
        .env_runners(num_env_runners=1)
        .resources(num_gpus=0, custom_resources_per_worker={"custom_resource": 0.01})
    )
    stop = {"training_iteration": 1}

    tune.Tuner(
        algorithm,
        param_space=config,
        run_config=air.RunConfig(stop=stop, verbose=0),
        tune_config=tune.TuneConfig(num_samples=1),
    ).fit()


@pytest.mark.parametrize("algorithm", ["PPO", "IMPALA"])
def test_algorithm_custom_resource(algorithm):
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
        .rollouts(num_rollout_workers=1)
        .resources(num_gpus=0, custom_resources={"custom_resource": 0.01})
    )
    stop = {"training_iteration": 1}

    tune.Tuner(
        algorithm,
        param_space=config,
        run_config=air.RunConfig(stop=stop, verbose=0),
        tune_config=tune.TuneConfig(num_samples=1),
    ).fit()


@pytest.mark.parametrize("algorithm", ["PPO", "IMPALA"])
def test_learner_custom_resource(algorithm):
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
        .rollouts(num_rollout_workers=1)
        .resources(num_gpus=0, custom_resources_per_learner_worker={"custom_resource": 0.01})
        .experimental(_enable_new_api_stack=True)
    )
    stop = {"training_iteration": 1}

    tune.Tuner(
        algorithm,
        param_space=config,
        run_config=air.RunConfig(stop=stop, verbose=0),
        tune_config=tune.TuneConfig(num_samples=1),
    ).fit()


def test_custom_resource_type():
    # CPU/GPU should not be specified in custom resources api
    algorithm = "PPO"

    config = (
        get_trainable_cls(algorithm)
        .get_default_config()
        .environment("CartPole-v1")
        .framework("torch")
        .rollouts(num_rollout_workers=1)
        .resources(num_gpus=0, custom_resources_per_learner_worker={"CPU": 1})
        .experimental(_enable_new_api_stack=True)
    )
    
    stop = {"training_iteration": 1}

    with pytest.raises(ValueError) as exc_info:
        tune.Tuner(
            algorithm,
            param_space=config,
            run_config=air.RunConfig(stop=stop, verbose=0),
            tune_config=tune.TuneConfig(num_samples=1),
        ).fit()
    
    assert "Use the `num_cpus_per_learner_worker` " in str(exc_info.value)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
