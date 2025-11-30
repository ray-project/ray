from ray import tune
from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.tune import Stopper

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
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 15000,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 3000000,
    },
    "Hopper-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 3500,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 1000000,
    },
    "Humanoid-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 8000,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 10000000,
    },
    "Ant-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 5500,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 3000000,
    },
    "Walker2d-v4": {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 6000,
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 3000000,
    },
}


# Define a `tune.Stopper` that stops the training if the benchmark is reached
# or the maximum number of timesteps is exceeded.
class BenchmarkStopper(Stopper):
    def __init__(self, benchmark_envs):
        self.benchmark_envs = benchmark_envs

    def __call__(self, trial_id, result):
        # Stop training if the mean reward is reached.
        if (
            result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
            >= self.benchmark_envs[result["env"]][
                f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
            ]
        ):
            return True
        # Otherwise check, if the total number of timesteps is exceeded.
        elif (
            result[f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}"]
            >= self.benchmark_envs[result["env"]][f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}"]
        ):
            return True
        # Otherwise continue training.
        else:
            return False

    # Note, this needs to implemented b/c the parent class is abstract.
    def stop_all(self):
        return False


config = (
    SACConfig()
    .environment(env=tune.grid_search(list(benchmark_envs.keys())))
    .env_runners(
        rollout_fragment_length=1,
        num_env_runners=0,
    )
    .learners(
        # Note, we have a sample/train ratio of 1:1 and a small train
        # batch, so 1 learner with a single GPU should suffice.
        num_learners=1,
        num_gpus_per_learner=1,
    )
    # TODO (simon): Adjust to new model_config_dict.
    .training(
        initial_alpha=1.001,
        # Choose a smaller learning rate for the actor (policy).
        actor_lr=3e-5,
        critic_lr=3e-4,
        alpha_lr=1e-4,
        target_entropy="auto",
        n_step=1,
        tau=0.005,
        train_batch_size=256,
        target_network_update_freq=1,
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
        stop=BenchmarkStopper(benchmark_envs=benchmark_envs),
        name="benchmark_sac_mujoco",
    ),
)

tuner.fit()
