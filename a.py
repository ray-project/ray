from ray.rllib.algorithms.ppo.ppo import PPO, PPOConfig

config = (
    PPOConfig()
    .environment(env="CartPole-v0")
)
algo = PPO(config=config)
#del algo

