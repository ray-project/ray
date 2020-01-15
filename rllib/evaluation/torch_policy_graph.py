from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.utils import renamed_class

TorchPolicyGraph = renamed_class(TorchPolicy, old_name="TorchPolicyGraph")
