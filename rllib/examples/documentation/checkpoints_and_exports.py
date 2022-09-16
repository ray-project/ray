# Create a PPO algorithm object ..
from ray.rllib.algorithms.ppo import PPOConfig

my_ppo = PPOConfig().environment(env="CartPole-v0").build()
# .. train one iteration ..
my_ppo.train()
# .. and call `save()` to create a checkpoint.
path_to_checkpoint = my_ppo.save()

# Let's terminate the algo for demonstration purposes.
my_ppo.stop()
# Doing this will lead to an error.
# my_ppo.train()
