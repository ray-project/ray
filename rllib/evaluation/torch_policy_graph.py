from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.utils import renamed_class

TorchPolicyGraph = renamed_class(TorchPolicy, old_name="TorchPolicyGraph")
