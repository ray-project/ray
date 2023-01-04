import gymnasium as gym
from gymnasium.wrappers import TimeLimit

from ray.rllib.algorithms.ppo import PPOConfig

gym.register(
    "cartpole_truncated",
    lambda _: TimeLimit(gym.make("CartPole-v1"), max_episode_steps=50),
)

config = (
    PPOConfig()
    .environment("cartpole_truncated")
    .rollouts(num_envs_per_worker=10)
    .evaluation(
        evaluation_config=PPOConfig.overrides(env="CartPole-v1"),
        evaluation_interval=1,
        evaluation_num_workers=1,
    )
)

stop = {
    "timesteps_total": 500000,
    "evaluation/episode_reward_mean": 200.0,
}
