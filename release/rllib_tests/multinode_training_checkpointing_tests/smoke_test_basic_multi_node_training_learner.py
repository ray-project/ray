import ray
from ray import air, tune
from ray.rllib.algorithms.ppo import PPOConfig


def run_with_tuner_n_rollout_worker_2_gpu(config):
    """Run training with n rollout workers and 2 learner workers with gpu."""
    config = config.rollouts(num_rollout_workers=5)
    tuner = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=air.RunConfig(
            stop={"timesteps_total": 128},
            failure_config=air.FailureConfig(fail_fast=True),
        ),
    )
    tuner.fit()


def run_with_tuner_0_rollout_worker_2_gpu(config):
    """Run training with 0 rollout workers with 2 learner workers with gpu."""
    config = config.rollouts(num_rollout_workers=0)
    tuner = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=air.RunConfig(
            stop={"timesteps_total": 128},
            failure_config=air.FailureConfig(fail_fast=True),
        ),
    )
    tuner.fit()


def run_tuner_n_rollout_workers_0_gpu(config):
    """Run training with n rollout workers, multiple learner workers, and no gpu."""
    config = config.rollouts(num_rollout_workers=5)
    config = config.resources(
        num_cpus_per_learner_worker=1,
        num_learner_workers=2,
    )

    tuner = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=air.RunConfig(
            stop={"timesteps_total": 128},
            failure_config=air.FailureConfig(fail_fast=True),
        ),
    )
    tuner.fit()


def run_tuner_n_rollout_workers_1_gpu_local(config):
    """Run training with n rollout workers, local learner, and 1 gpu."""
    config = config.rollouts(num_rollout_workers=5)
    config = config.resources(
        num_gpus_per_learner_worker=1,
        num_learner_workers=0,
    )

    tuner = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=air.RunConfig(
            stop={"timesteps_total": 128},
            failure_config=air.FailureConfig(fail_fast=True),
        ),
    )
    tuner.fit()


def test_multi_node_training_smoke():
    """A smoke test to see if we can run multi node training without pg problems.

    This test is run on a 3 node cluster. The head node is a m5.xlarge (4 cpu),
    the worker nodes are 2 g4dn.xlarge (1 gpu, 4 cpu) machines.

    """

    ray.init()

    config = (
        PPOConfig()
        .training(
            _enable_learner_api=True,
            model={
                "fcnet_hiddens": [256, 256, 256],
                "fcnet_activation": "relu",
                "vf_share_layers": True,
            },
            train_batch_size=128,
        )
        .rl_module(_enable_rl_module_api=True)
        .environment("CartPole-v1")
        .resources(
            num_gpus_per_learner_worker=1,
            num_learner_workers=2,
        )
        .rollouts(num_rollout_workers=2)
        .reporting(min_time_s_per_iteration=0, min_sample_timesteps_per_iteration=10)
    )
    for fw in ["tf2", "torch"]:
        config = config.framework(fw, eager_tracing=True)

        run_with_tuner_0_rollout_worker_2_gpu(config)
        run_with_tuner_n_rollout_worker_2_gpu(config)
        run_tuner_n_rollout_workers_0_gpu(config)
        run_tuner_n_rollout_workers_1_gpu_local(config)


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
