from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils import renamed_class

SampleBatch = renamed_class(
    SampleBatch, old_name="rllib.evaluation.SampleBatch")
MultiAgentBatch = renamed_class(
    MultiAgentBatch, old_name="rllib.evaluation.MultiAgentBatch")
