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

my_new_ppo.stop()

# __restore-from-algo-checkpoint-2-begin__

# Re-build a fresh algorithm.
my_new_ppo = my_ppo_config.build()

# Restore the old (checkpointed) state.
my_new_ppo.restore(path_to_checkpoint)

# Continue training.
my_new_ppo.train()

# __restore-from-algo-checkpoint-2-end__

my_new_ppo.stop()

# __multi-agent-checkpoints-begin__

import os

# Use our example multi-agent CartPole environment to train in.
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole

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

# Add the MultiAgentCartPole env to our config and build our Algorithm.
my_ma_config.environment(env=MultiAgentCartPole, env_config={
    "num_agents": 2,
})

my_ma_algo = my_ma_config.build()

ma_checkpoint_dir = my_ma_algo.save()

print(
    "An Algorithm checkpoint has been created inside directory: "
    f"'{ma_checkpoint_dir}'.\n"
    "Individual Policy checkpoints can be found in "
    f"'{os.path.join(ma_checkpoint_dir, 'policies')}'."
)

# __multi-agent-checkpoints-end__

# __create-policy-checkpoint-begin__

# Retrieve the Policy object from an Algorithm.
# Note that for normal, single-agent Algorithms, the Policy ID is "default_policy".
policy1 = my_ma_algo.get_policy(policy_id="pol1")

# Tell RLlib to store an individual policy checkpoint (only for "pol1") inside
# /tmp/my_policy_checkpoint
policy1.export_checkpoint("/tmp/my_policy_checkpoint")

# __create-policy-checkpoint-end__

# __restore-policy-begin__

import numpy as np

from ray.rllib.policy.policy import Policy

# Use the `from_checkpoint` utility of the Policy class:
my_restored_policy = Policy.from_checkpoint("/tmp/my_policy_checkpoint")

# Use the restored policy for serving actions.
obs = np.array([0.0, 0.1, 0.2, 0.3])  # individual CartPole observation
action = my_restored_policy.compute_single_action(obs)

print(f"Computed action {action} from given CartPole observation.")

# __restore-policy-end__
