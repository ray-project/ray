from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.tune import Stopper
from ray import train, tune

# Needs the following packages to be installed on Ubuntu:
#   sudo apt-get libosmesa-dev
#   sudo apt-get install patchelf
#   python -m pip install "gymnasium[mujoco]"
# Might need to be added to bashsrc:
#   export MUJOCO_GL=osmesa"
#   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/.mujoco/mujoco200/bin"

# See the following links for becnhmark results of other libraries:
#   Original paper: https://arxiv.org/pdf/1707.06347
#   CleanRL: https://wandb.ai/openrlbenchmark/openrlbenchmark/reports"
#       /MuJoCo-CleanRL-s-PPO--VmlldzoxODAwNjkw
#   AgileRL: https://github.com/AgileRL/AgileRL?tab=readme-ov-file#benchmarks
benchmark_envs = {
    "HalfCheetah-v4": {
        "sampler_results/episode_reward_mean": 2000,
        "timesteps_total": 1000000,
    },
    "Hopper-v4": {
        "sampler_results/episode_reward_mean": 2250,
        "timesteps_total": 1000000,
    },
    "InvertedPendulum-v4": {
        "sampler_results/episode_reward_mean": 1000,
        "timesteps_total": 1000000,
    },
    "InvertedDoublePendulum-v4": {
        "sampler_results/episode_reward_mean": 8000,
        "timesteps_total": 1000000,
    },
    "Reacher-v4": {
        "sampler_results/episode_reward_mean": -15,
        "timesteps_total": 1000000,
    },
    "Swimmer-v4": {
        "sampler_results/episode_reward_mean": 120,
        "timesteps_total": 1000000,
    },
    "Walker2d-v4": {
        "sampler_results/episode_reward_mean": 3500,
        "timesteps_total": 1000000,
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
            result["sampler_results"]["episode_reward_mean"]
            >= self.benchmark_envs[result["env"]]["sampler_results/episode_reward_mean"]
        ):
            return True
        # Otherwise check, if the total number of timesteps is exceeded.
        elif (
            result["timesteps_total"]
            >= self.benchmark_envs[result["env"]]["timesteps_total"]
        ):
            return True
        # Otherwise continue training.
        else:
            return False

    # Note, this needs to implemented b/c the parent class is abstract.
    def stop_all(self):
        return False


config = (
    PPOConfig()
    .environment(env=tune.grid_search(list(benchmark_envs.keys())))
    # Enable new API stack and use EnvRunner.
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        # Following the paper.
        num_rollout_workers=32,
        rollout_fragment_length=512,
    )
    .resources(
        # Let's start with a small number of learner workers and
        # add later a tune grid search for these resources.
        num_learner_workers=1,
        num_gpus_per_learner_worker=1,
    )
    # TODO (simon): Adjust to new model_config_dict.
    .training(
        # Following the paper.
        gamma=0.99,
        lambda_=0.95,
        lr=0.0003,
        num_sgd_iter=15,
        train_batch_size=32 * 512,
        sgd_minibatch_size=4096,
        vf_loss_coeff=0.01,
        model={
            "fcnet_hiddens": [64, 64],
            "fcnet_activation": "tanh",
            "vf_share_layers": True,
        },
    )
    .reporting(
        metrics_num_episodes_for_smoothing=5,
        min_sample_timesteps_per_iteration=1000,
    )
    .evaluation(
        evaluation_duration="auto",
        evaluation_interval=1,
        evaluation_num_workers=1,
        evaluation_parallel_to_training=True,
        evaluation_config={
            "explore": True,
        },
    )
)

tuner = tune.Tuner(
    "PPO",
    param_space=config,
    run_config=train.RunConfig(
        stop=BenchmarkStopper(benchmark_envs=benchmark_envs),
        name="benchmark_ppo_mujoco",
    ),
)

tuner.fit()
