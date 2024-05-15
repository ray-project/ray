import gymnasium as gym
from gymnasium.wrappers import TimeLimit

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.tune.registry import register_env


# For training, use a time-truncated (max. 50 timestep) version of CartPole-v1.
register_env(
    "cartpole_truncated",
    lambda _: TimeLimit(gym.make("CartPole-v1"), max_episode_steps=50),
)

config = (
    PPOConfig()
    .environment("cartpole_truncated")
    .env_runners(num_envs_per_env_runner=10)
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
