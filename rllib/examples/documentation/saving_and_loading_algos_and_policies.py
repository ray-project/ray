# flake8: noqa

# __create-algo-checkpoint-begin__
# Create a PPO algorithm object using a config object ..
from ray.rllib.algorithms.ppo import PPOConfig

my_ppo_config = PPOConfig().environment("CartPole-v1")
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
        lambda agent_id, episode, worker, **kw: (
            "pol1" if agent_id == "agent1" else "pol2"
        )
    ),
    # Setting these is not necessary. All policies will always be trained by default.
    # However, since we do provide a list of IDs here, we need to remain in charge of
    # changing this `policies_to_train` list, should we ever alter the Algorithm
    # (e.g. remove one of the policies or add a new one).
    policies_to_train=["pol1", "pol2"],  # Again, `None` would be totally fine here.
)

# Add the MultiAgentCartPole env to our config and build our Algorithm.
my_ma_config.environment(
    MultiAgentCartPole,
    env_config={
        "num_agents": 2,
    },
)

my_ma_algo = my_ma_config.build()

ma_checkpoint_dir = my_ma_algo.save()

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

# Make sure, pol2 is NOT in this Algorithm anymore.
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
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole

# Set up an Algorithm with 5 Policies.
algo_w_5_policies = (
    PPOConfig()
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
path_to_checkpoint = algo_w_5_policies.save()
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


# __export-models-begin__
from ray.rllib.algorithms.ppo import PPOConfig

# Create a new Algorithm (which contains a Policy, which contains a NN Model).
# Switch on for native models to be included in the Policy checkpoints.
ppo_config = (
    PPOConfig().environment("Pendulum-v1").checkpointing(export_native_model_files=True)
)

# The default framework is TensorFlow, but if you would like to do this example with
# PyTorch, uncomment the following line of code:
# ppo_config.framework("torch")

# Create the Algorithm and train one iteration.
ppo = ppo_config.build()
ppo.train()

# Get the underlying PPOTF1Policy (or PPOTorchPolicy) object.
ppo_policy = ppo.get_policy()

# __export-models-end__

# Export the Keras NN model (that our PPOTF1Policy inside the PPO Algorithm uses)
# to disk ...

# 1) .. using the Policy object:

# __export-models-1-begin__
ppo_policy.export_model("/tmp/my_nn_model")
# .. check /tmp/my_nn_model/ for the keras model files. You should be able to recover
# the keras model via:
# keras_model = tf.saved_model.load("/tmp/my_nn_model/")
# And pass in a Pendulum-v1 observation:
# results = keras_model(tf.convert_to_tensor(
#     np.array([[0.0, 0.1, 0.2]]), dtype=np.float32)
# )

# For PyTorch, do:
# pytorch_model = torch.load("/tmp/my_nn_model/model.pt")
# results = pytorch_model(
#     input_dict={
#         "obs": torch.from_numpy(np.array([[0.0, 0.1, 0.2]], dtype=np.float32)),
#     },
#     state=[torch.tensor(0)],  # dummy value
#     seq_lens=torch.tensor(0),  # dummy value
# )

# __export-models-1-end__

# 2) .. via the Policy's checkpointing method:

# __export-models-2-begin__
checkpoint_dir = ppo_policy.export_checkpoint("tmp/ppo_policy")
# .. check /tmp/ppo_policy/model/ for the keras model files.
# You should be able to recover the keras model via:
# keras_model = tf.saved_model.load("/tmp/ppo_policy/model")
# And pass in a Pendulum-v1 observation:
# results = keras_model(tf.convert_to_tensor(
#     np.array([[0.0, 0.1, 0.2]]), dtype=np.float32)
# )

# __export-models-2-end__

# 3) .. via the Algorithm (Policy) checkpoint:

# __export-models-3-begin__
checkpoint_dir = ppo.save()
# .. check `checkpoint_dir` for the Algorithm checkpoint files.
# You should be able to recover the keras model via:
# keras_model = tf.saved_model.load(checkpoint_dir + "/policies/default_policy/model/")
# And pass in a Pendulum-v1 observation
# results = keras_model(tf.convert_to_tensor(
#     np.array([[0.0, 0.1, 0.2]]), dtype=np.float32)
# )

# __export-models-3-end__


# __export-models-as-onnx-begin__
# Using the same Policy object, we can also export our NN Model in the ONNX format:
ppo_policy.export_model("/tmp/my_nn_model", onnx=True)

# __export-models-as-onnx-end__
