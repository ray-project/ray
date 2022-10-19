from ray.rllib.algorithms.a2c import A2CConfig


config = A2CConfig().training(lr=0.001)\
    .framework("tf")\
    .rollouts(num_rollout_workers=0)\
    .environment(env="CartPole-v1")\

stop = {"episode_reward_mean": 150, "timesteps_total": 500000}
