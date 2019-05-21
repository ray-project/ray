from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.policy.policy import Policy
from ray.rllib.utils import renamed_class

PolicyGraph = renamed_class(Policy, old_name="PolicyGraph")
