from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils import renamed_class

TFPolicyGraph = renamed_class(TFPolicy, old_name="TFPolicyGraph")
