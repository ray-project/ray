from ray.rllib.policy.policy import Policy
from ray.rllib.utils import renamed_class

PolicyGraph = renamed_class(Policy, old_name="PolicyGraph")
