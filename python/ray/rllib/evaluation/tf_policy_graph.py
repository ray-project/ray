from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils import renamed_class

TFPolicyGraph = renamed_class(TFPolicy, old_name="TFPolicyGraph")
