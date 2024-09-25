import gymnasium as gym
from gymnasium.wrappers import TimeLimit

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

parser = add_rllib_example_script_args()
parser.set_defaults(enable_new_api_stack=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

# For training, use a time-truncated (max. 50 timestep) version of CartPole-v1.
register_env(
    "cartpole_truncated",
    lambda _: TimeLimit(gym.make("CartPole-v1"), max_episode_steps=50),
)

config = (
    PPOConfig()
    # Enable new API stack and use EnvRunner.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment("cartpole_truncated")
    .env_runners(num_envs_per_env_runner=10)
    .training(
        lr=0.0003,
        num_epochs=6,
        vf_loss_coeff=0.01,
    )
    # For evaluation, use the "real" CartPole-v1 env (up to 500 steps).
    .evaluation(
        evaluation_config=PPOConfig.overrides(env="CartPole-v1"),
        evaluation_interval=1,
        evaluation_num_env_runners=1,
    )
)

stop = {
    f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 500000,
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 200.0,
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)
