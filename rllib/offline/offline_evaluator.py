import logging
from typing import Dict, Any

from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import (
    DEFAULT_POLICY_ID,
    MultiAgentBatch,
    SampleBatch,
)
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import SampleBatchType

logger = logging.getLogger(__name__)


@DeveloperAPI
class OfflineEvaluator:
    """Interface for an offline evaluator of a policy"""

    @DeveloperAPI
    def __init__(self, policy: Policy, **kwargs):
        """Initializes an OffPolicyEstimator instance.

        Args:
            policy: Policy to evaluate.
            kwargs: forward compatibility placeholder.
        """
        self.policy = policy

    @DeveloperAPI
    def estimate(self, batch: SampleBatchType, **kwargs) -> Dict[str, Any]:
        """Returns the evaluation results for the given batch of episodes.

        Args:
            batch: The batch to evaluate.
            kwargs: forward compatibility placeholder.

        Returns:
            The evaluation done on the given batch. The returned
            dict can be any arbitrary mapping of strings to metrics.
        """
        raise NotImplementedError

    @DeveloperAPI
    def train(self, batch: SampleBatchType, **kwargs) -> Dict[str, Any]:
        """Sometimes you need to train a model inside an evaluator. This method
        abstracts the training process.

        Args:
            batch: SampleBatch to train on
            kwargs: forward compatibility placeholder.

        Returns:
            Any optional metrics to return from the evaluator
        """
        return {}

    @DeveloperAPI
    def convert_ma_batch_to_sample_batch(self, batch: SampleBatchType) -> SampleBatch:
        """Converts a MultiAgentBatch to a SampleBatch if neccessary.

        Args:
            batch: The SampleBatchType to convert.

        Returns:
            batch: the converted SampleBatch

        Raises:
            ValueError if the MultiAgentBatch has more than one policy_id
            or if the policy_id is not `DEFAULT_POLICY_ID`
        """
        # TODO: Make this a util to sample_batch.py
        if isinstance(batch, MultiAgentBatch):
            policy_keys = batch.policy_batches.keys()
            if len(policy_keys) == 1 and DEFAULT_POLICY_ID in policy_keys:
                batch = batch.policy_batches[DEFAULT_POLICY_ID]
            else:
                raise ValueError(
                    "Off-Policy Estimation is not implemented for "
                    "multi-agent batches. You can set "
                    "`off_policy_estimation_methods: {}` to resolve this."
                )
        return batch
