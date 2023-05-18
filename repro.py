import ray
from ray import air, tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.env.dm_control_suite import cheetah_run


def run_with_tuner_1_rollout_worker(config):
    config = config.rollouts(num_rollout_workers=1)
    print("-" * 80)
    tuner = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=air.RunConfig(
            stop={"timesteps_total": 128},
            failure_config=air.FailureConfig(max_failures=3),
            # storage_path="/mnt/shared_storage/avnishn/ppo_multi_gpu_benchmarking",
            # checkpoint_config=air.CheckpointConfig(checkpoint_frequency=1),
            # sync_config=tune.SyncConfig(syncer=None)
        ),
    )
    tuner.fit()


def run_with_tuner_0_rollout_worker(config):
    print("-" * 80)
    config = config.rollouts(num_rollout_workers=0)
    tuner = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=air.RunConfig(
            stop={"timesteps_total": 128},
            failure_config=air.FailureConfig(max_failures=3),
            # storage_path="/mnt/shared_storage/avnishn/ppo_multi_gpu_benchmarking",
            # checkpoint_config=air.CheckpointConfig(checkpoint_frequency=1),
            # sync_config=tune.SyncConfig(syncer=None)
        ),
    )
    tuner.fit()


if __name__ == "__main__":

    # This experiment is run on a machine with a 8 cpu headnode, and 2, 1 gpu 4 cpu
    # workernodes. Note I couldn't reproduce this bug if I made my worker nodes 2,
    # 4 gpu 32 cpu instances.

    ray.init()

    tune.registry.register_env(
        "HalfCheetahDmc", lambda c: cheetah_run(from_pixels=False)
    )

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
        .environment("HalfCheetahDmc")
        .resources(
            num_gpus_per_learner_worker=1,
            num_learner_workers=2,
        )
        .rollouts(num_rollout_workers=1)
        .reporting(min_time_s_per_iteration=0, min_sample_timesteps_per_iteration=10)
    )

    # run_with_tuner_0_rollout_worker(config)  # this works
    print("finished without tune")
    print("*" * 100)
    run_with_tuner_1_rollout_worker(config)  # this hangs
