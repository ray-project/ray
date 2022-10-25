from ray.rllib.algorithms.simple_q import SimpleQConfig


config = (
    SimpleQConfig()
    .environment("CartPole-v1")
    .framework("tf")
    .rollouts(num_rollout_workers=0)
)
stop = {"episode_reward_mean": 15, "timesteps_total": 500}
