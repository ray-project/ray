from ray.rllib.algorithms.ppo import PPOConfig
from ray.tune.registry import register_env


def env_creator(ctx):
    import gymnasium as gym
    from gymnasium.wrappers import TimeLimit

    return TimeLimit(gym.make("CartPole-v1"), max_episode_steps=50)


register_env("cartpole-truncated", env_creator)


config = (
    PPOConfig()
    .environment("cartpole-truncated")
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
