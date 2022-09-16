# __create-algo-checkpoint-begin__
# Create a PPO algorithm object ..
from ray.rllib.algorithms.ppo import PPOConfig

my_ppo = PPOConfig().environment(env="CartPole-v0").build()
# .. train one iteration ..
my_ppo.train()
# .. and call `save()` to create a checkpoint.
path_to_checkpoint = my_ppo.save()
print(
    "An Algorithm checkpoint has been created inside directory: "
    f"'{path_to_checkpoint}'."
)

# Let's terminate the algo for demonstration purposes.
my_ppo.stop()
# Doing this will lead to an error.
# my_ppo.train()
# __create-algo-checkpoint-end__


# __restore-from-algo-checkpoint-begin__
from ray.rllib.algorithms.algorithm import Algorithm

# Use the Algorithm's `from_checkpoint` utility to get a new algo instance
# that has the exact same state as the old one, from which the checkpoint was
# created in the first place:
my_new_ppo = Algorithm.from_checkpoint(path_to_checkpoint)

# Continue training.
my_new_ppo.train()

# __restore-from-algo-checkpoint-end__
