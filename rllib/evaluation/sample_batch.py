from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils import renamed_class

SampleBatch = renamed_class(
    SampleBatch, old_name="rllib.evaluation.SampleBatch")
MultiAgentBatch = renamed_class(
    MultiAgentBatch, old_name="rllib.evaluation.MultiAgentBatch")
