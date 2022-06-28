from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import TensorType

def lookup_state_value_fn(policy: Policy):
    raise NotImplementedError(f"Could not look up state value function for policy: {str(policy)}")

def lookup_action_value_fn(policy: Policy):
    raise NotImplementedError(f"Could not look up action value function for policy: {str(policy)}")