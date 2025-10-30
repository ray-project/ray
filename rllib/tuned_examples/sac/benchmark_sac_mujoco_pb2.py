import time

from ray import tune
from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.tune.schedulers.pb2 import PB2

# Needs the following packages to be installed on Ubuntu:
#   sudo apt-get libosmesa-dev
#   sudo apt-get install patchelf
#   python -m pip install "gymnasium[mujoco]"
# Might need to be added to bashsrc:
#   export MUJOCO_GL=osmesa"
#   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/.mujoco/mujoco200/bin"

# See the following links for becnhmark results of other libraries:
#   Original paper: https://arxiv.org/abs/1812.05905
#   CleanRL: https://wandb.ai/cleanrl/cleanrl.benchmark/reports/Mujoco--VmlldzoxODE0NjE
#   AgileRL: https://github.com/AgileRL/AgileRL?tab=readme-ov-file#benchmarks
benchmark_envs = {
    "HalfCheetah-v4": {
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 3000000,
    },
    "Hopper-v4": {
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 1000000,
    },
    "Humanoid-v4": {
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 10000000,
    },
    "Ant-v4": {f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 3000000},
    "Walker2d-v4": {
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 3000000,
    },
}

pb2_scheduler = PB2(
    time_attr=NUM_ENV_STEPS_SAMPLED_LIFETIME,
    metric=f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}",
    mode="max",
    perturbation_interval=50000,
    # Copy bottom % with top % weights.
    quantile_fraction=0.25,
    hyperparam_bounds={
        "actor_lr": [1e-5, 1e-3],
        "critic_lr": [1e-6, 1e-4],
        "alpha_lr": [1e-6, 1e-3],
        "gamma": [0.95, 0.99],
        "n_step": [1, 3],
        "initial_alpha": [1.0, 1.5],
        "tau": [0.001, 0.1],
        "target_entropy": [-10, -1],
        "train_batch_size": [128, 512],
        "target_network_update_freq": [1, 4],
    },
)

experiment_start_time = time.time()
for env, stop_criteria in benchmark_envs.items():
    hp_trial_start_time = time.time()
    config = (
        SACConfig()
        .environment(env=env)
        .env_runners(
            rollout_fragment_length="auto",
            num_env_runners=1,
            # TODO (sven, simon): Add resources.
        )
        .learners(
            # Note, we have a small batch and a sample/train ratio
            # of 1:1, so a single GPU should be enough.
            num_learners=1,
            num_gpus_per_learner=1,
        )
        # TODO (simon): Adjust to new model_config_dict.
        .training(
            initial_alpha=tune.choice([1.0, 1.5]),
            actor_lr=tune.uniform(1e-5, 1e-3),
            critic_lr=tune.uniform([1e-6, 1e-4]),
            alpha_lr=tune.uniform([1e-6, 1e-3]),
            target_entropy=tune.choice([-10, -5, -1, "auto"]),
            n_step=tune.choice([1, 3, (1, 3)]),
            tau=tune.uniform(0.001, 0.1),
            train_batch_size=tune.choice([128, 256, 512]),
            target_network_update_freq=tune.choice([1, 2, 4]),
            replay_buffer_config={
                "type": "PrioritizedEpisodeReplayBuffer",
                "capacity": 1000000,
                "alpha": 0.6,
                "beta": 0.4,
            },
            num_steps_sampled_before_learning_starts=256,
            model={
                "fcnet_hiddens": [256, 256],
                "fcnet_activation": "relu",
                "post_fcnet_hiddens": [],
                "post_fcnet_activation": None,
                "post_fcnet_weights_initializer": "orthogonal_",
                "post_fcnet_weights_initializer_config": {"gain": 0.01},
            },
        )
        .reporting(
            metrics_num_episodes_for_smoothing=5,
            min_sample_timesteps_per_iteration=1000,
        )
        .evaluation(
            evaluation_duration="auto",
            evaluation_interval=1,
            evaluation_num_env_runners=1,
            evaluation_parallel_to_training=True,
            evaluation_config={
                "explore": False,
            },
        )
    )

    tuner = tune.Tuner(
        "SAC",
        param_space=config,
        run_config=tune.RunConfig(
            stop=stop_criteria,
            name="benchmark_sac_mujoco_pb2_" + env,
        ),
        tune_config=tune.TuneConfig(
            scheduler=pb2_scheduler,
            num_samples=8,
        ),
    )
    result_grid = tuner.fit()
    best_result = result_grid.get_best_result()
    print(
        f"Finished running HP search for (env={env}) in "
        f"{time.time() - hp_trial_start_time} seconds."
    )
    print(f"Best result for {env}: {best_result}")
    print(f"Best config for {env}: {best_result['config']}")

    # Run again with the best config.
    best_trial_start_time = time.time()
    tuner = tune.Tuner(
        "SAC",
        param_space=best_result.config,
        run_config=tune.RunConfig(
            stop=stop_criteria,
            name="benchmark_sac_mujoco_pb2_" + env + "_best",
        ),
    )
    print(f"Running best config for (env={env})...")
    tuner.fit()
    print(
        f"Finished running best config for (env={env}) "
        f"in {time.time() - best_trial_start_time} seconds."
    )

print(
    f"Finished running HP search on all MuJoCo benchmarks in "
    f"{time.time() - experiment_start_time} seconds."
)
print(
    "Results from running the best configs can be found in the "
    "`benchmark_sac_mujoco_pb2_<ENV-NAME>_best` directories."
)
