# Run with:
# rllib train file cartpole_a2c.py \
#     --stop={'timesteps_total': 50000, 'episode_reward_mean': 200}"
from ray.rllib.algorithms.a2c import A2CConfig


config = (
    A2CConfig()
    .environment("CartPole-v1")
    .training(lr=0.001, train_batch_size=20)
    .framework("tf")
    .rollouts(num_rollout_workers=0)
)
