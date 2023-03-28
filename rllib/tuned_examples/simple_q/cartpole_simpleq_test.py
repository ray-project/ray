# Run with:
# rllib train -f cartpole_simpleq_test.py\
#     --stop={'timesteps_total': 50000, 'episode_reward_mean': 200}"
from ray.rllib.algorithms.simple_q import SimpleQConfig


config = (
    SimpleQConfig()
    .environment("CartPole-v1")
    .framework("tf")
    .rollouts(num_rollout_workers=0)
)
