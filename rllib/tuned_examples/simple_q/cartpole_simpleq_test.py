from ray.rllib.algorithms.simple_q import SimpleQConfig


config = (
    SimpleQConfig()
    .framework("tf")
    .rollouts(num_rollout_workers=0)
    .environment(env="CartPole-v1")
)
stop = {"episode_reward_mean": 15, "timesteps_total": 500}
