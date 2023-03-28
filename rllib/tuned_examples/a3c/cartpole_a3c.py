# Run with:
# rllib train file cartpole_a3c.py \
#     --stop={'timesteps_total': 20000, 'episode_reward_mean': 150}"
from ray.rllib.algorithms.a3c import A3CConfig


config = (
    A3CConfig()
    .training(gamma=0.95)
    .environment("CartPole-v1")
    .framework("tf")
    .rollouts(num_rollout_workers=0)
)
