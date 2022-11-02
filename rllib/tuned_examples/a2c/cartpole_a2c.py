from ray.rllib.algorithms.a2c import A2CConfig


config = (
    A2CConfig()
    .environment("CartPole-v1")
    .training(lr=0.001)
    .framework("tf")
    .rollouts(num_rollout_workers=0)
)
stop = {"episode_reward_mean": 150, "timesteps_total": 500000}
