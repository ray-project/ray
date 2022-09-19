# __create-algo-checkpoint-begin__

# Create a PPO algorithm object using a config object ..
from ray.rllib.algorithms.ppo import PPOConfig

my_ppo_config = PPOConfig().environment(env="CartPole-v0")
my_ppo = my_ppo_config.build()

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

# __restore-from-algo-checkpoint-2-begin__

# Re-build a fresh algorithm.
my_new_ppo = my_ppo_config.build()

# Restore the old (checkpointed) state.
my_new_ppo.restore(path_to_checkpoint)

# Continue training.
my_new_ppo.train()

# __restore-from-algo-checkpoint-2-end__


# __multi-agent-checkpoints-begin__

# Set up a multi-agent Algorithm, training two policies independently.
my_ma_config = PPOConfig().multi_agent(
    # Which policies should RLlib create and train?
    policies={"pol1", "pol2"},
    # Let RLlib know, which agents in the environment (we'll have "agent1"
    # and "agent2") map to which policies.
    policy_mapping_fn=(
        lambda agent_id, worker, episode, **kw: (
            "pol1" if agent_id == "agent1"
            else "pol2"
        )
    ),
    # Setting these is not necessary. All policies will be trained anyways by default.
    policies_to_train=["pol1", "pol2"],
)
# Use our example multi-agent CartPole environment to train in.
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
my_ma_config.environment(env=MultiAgentCartPole, env_config={
    "num_agents": 2,
})



# __multi-agent-checkpoints-end__
