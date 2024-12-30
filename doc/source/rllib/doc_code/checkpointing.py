# flake8: noqa

# __multi-agent-checkpoints-begin__
import os

# Use our example multi-agent CartPole environment to train in.
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole

# Set up a multi-agent Algorithm, training two policies independently.
my_ma_config = (
    PPOConfig()
    .api_stack(
        enable_rl_module_and_learner=False,
        enable_env_runner_and_connector_v2=False,
    )
    .multi_agent(
        # Which policies should RLlib create and train?
        policies={"pol1", "pol2"},
        # Let RLlib know, which agents in the environment (we'll have "agent1"
        # and "agent2") map to which policies.
        policy_mapping_fn=(
            lambda agent_id, episode, worker, **kw: (
                "pol1" if agent_id == "agent1" else "pol2"
            )
        ),
        # Setting these isn't necessary. All policies will always be trained by default.
        # However, since we do provide a list of IDs here, we need to remain in charge of
        # changing this `policies_to_train` list, should we ever alter the Algorithm
        # (e.g. remove one of the policies or add a new one).
        policies_to_train=["pol1", "pol2"],  # Again, `None` would be totally fine here.
    )
)

# Add the MultiAgentCartPole env to our config and build our Algorithm.
my_ma_config.environment(
    MultiAgentCartPole,
    env_config={
        "num_agents": 2,
    },
)

my_ma_algo = my_ma_config.build()
my_ma_algo.train()

ma_checkpoint_dir = my_ma_algo.save().checkpoint.path

print(
    "An Algorithm checkpoint has been created inside directory: "
    f"'{ma_checkpoint_dir}'.\n"
    "Individual Policy checkpoints can be found in "
    f"'{os.path.join(ma_checkpoint_dir, 'policies')}'."
)

# Create a new Algorithm instance from the above checkpoint, just as you would for
# a single-agent setup:
my_ma_algo_clone = Algorithm.from_checkpoint(ma_checkpoint_dir)

# __multi-agent-checkpoints-end__

my_ma_algo_clone.stop()

# __multi-agent-checkpoints-restore-policy-sub-set-begin__
# Here, we use the same (multi-agent Algorithm) checkpoint as above, but only restore
# it with the first Policy ("pol1").

my_ma_algo_only_pol1 = Algorithm.from_checkpoint(
    ma_checkpoint_dir,
    # Tell the `from_checkpoint` util to create a new Algo, but only with "pol1" in it.
    policy_ids=["pol1"],
    # Make sure to update the mapping function (we must not map to "pol2" anymore
    # to avoid a runtime error). Now both agents ("agent0" and "agent1") map to
    # the same policy.
    policy_mapping_fn=lambda agent_id, episode, worker, **kw: "pol1",
    # Since we defined this above, we have to re-define it here with the updated
    # PolicyIDs, otherwise, RLlib will throw an error (it will think that there is an
    # unknown PolicyID in this list ("pol2")).
    policies_to_train=["pol1"],
)

# Make sure, pol2 isn't in this Algorithm anymore.
assert my_ma_algo_only_pol1.get_policy("pol2") is None

# Continue training (only with pol1).
my_ma_algo_only_pol1.train()

# __multi-agent-checkpoints-restore-policy-sub-set-end__

my_ma_algo_only_pol1.stop()

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


# __restore-algorithm-from-checkpoint-with-fewer-policies-begin__
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole

# Set up an Algorithm with 5 Policies.
algo_w_5_policies = (
    PPOConfig()
    .api_stack(
        enable_rl_module_and_learner=False,
        enable_env_runner_and_connector_v2=False,
    )
    .environment(
        env=MultiAgentCartPole,
        env_config={
            "num_agents": 5,
        },
    )
    .multi_agent(
        policies={"pol0", "pol1", "pol2", "pol3", "pol4"},
        # Map "agent0" -> "pol0", etc...
        policy_mapping_fn=(
            lambda agent_id, episode, worker, **kwargs: f"pol{agent_id}"
        ),
    )
    .build()
)

# .. train one iteration ..
algo_w_5_policies.train()
# .. and call `save()` to create a checkpoint.
path_to_checkpoint = algo_w_5_policies.save().checkpoint.path
print(
    "An Algorithm checkpoint has been created inside directory: "
    f"'{path_to_checkpoint}'. It should contain 5 policies in the 'policies/' sub dir."
)
# Let's terminate the algo for demonstration purposes.
algo_w_5_policies.stop()

# We will now recreate a new algo from this checkpoint, but only with 2 of the
# original policies ("pol0" and "pol1"). Note that this will require us to change the
# `policy_mapping_fn` (instead of mapping 5 agents to 5 policies, we now have
# to map 5 agents to only 2 policies).


def new_policy_mapping_fn(agent_id, episode, worker, **kwargs):
    return "pol0" if agent_id in ["agent0", "agent1"] else "pol1"


algo_w_2_policies = Algorithm.from_checkpoint(
    checkpoint=path_to_checkpoint,
    policy_ids={"pol0", "pol1"},  # <- restore only those policy IDs here.
    policy_mapping_fn=new_policy_mapping_fn,  # <- use this new mapping fn.
)

# Test, whether we can train with this new setup.
algo_w_2_policies.train()
# Terminate the new algo.
algo_w_2_policies.stop()

# __restore-algorithm-from-checkpoint-with-fewer-policies-end__

