import gymnasium as gym
from gymnasium.wrappers import TimeLimit

from ray.rllib.algorithms.ppo import PPOConfig
from ray.tune.registry import register_env


# For training, use a time-truncated (max. 50 timestep) version of CartPole-v1.
register_env(
    "cartpole_truncated",
    lambda _: TimeLimit(gym.make("CartPole-v1"), max_episode_steps=50),
)

config = (
    PPOConfig()
    .environment("cartpole_truncated")
    .rollouts(num_envs_per_worker=10)
    # For evaluation, use the "real" CartPole-v1 env (up to 500 steps).
    .evaluation(
        evaluation_config=PPOConfig.overrides(env="CartPole-v1"),
        evaluation_interval=1,
        evaluation_num_workers=1,
    )
)

stop = {
    "timesteps_total": 500000,
    "evaluation/sampler_results/episode_reward_mean": 200.0,
}
