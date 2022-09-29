import logging
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import SampleBatchType
from typing import Dict, Any

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
