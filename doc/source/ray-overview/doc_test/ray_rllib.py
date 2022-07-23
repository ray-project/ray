from ray import tune
from ray.rllib.algorithms.ppo import PPO

tune.run(
    PPO,
    stop={"episode_len_mean": 20},
    config={"env": "CartPole-v0", "framework": "torch", "log_level": "INFO"},
)
