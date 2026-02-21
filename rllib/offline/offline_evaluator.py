import abc
import logging
import os
from typing import Any, Dict

from ray.data import Dataset
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import DeveloperAPI, ExperimentalAPI
from ray.rllib.utils.typing import SampleBatchType

logger = logging.getLogger(__name__)


@DeveloperAPI
class OfflineEvaluator(abc.ABC):
    """Interface for an offline evaluator of a policy"""

    @DeveloperAPI
    def __init__(self, policy: Policy, **kwargs):
        """Initializes an OffPolicyEstimator instance.

        Args:
            policy: Policy to evaluate.
            kwargs: forward compatibility placeholder.
        """
        self.policy = policy

    @abc.abstractmethod
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

    @ExperimentalAPI
    def estimate_on_dataset(
        self,
        dataset: Dataset,
        *,
        n_parallelism: int = os.cpu_count(),
    ) -> Dict[str, Any]:

        """Calculates the estimate of the metrics based on the given offline dataset.

        Typically, the dataset is passed through only once via n_parallel tasks in
        mini-batches to improve the run-time of metric estimation.

        Args:
            dataset: The ray dataset object to do offline evaluation on.
            n_parallelism: The number of parallelism to use for the computation.

        Returns:
            Dict[str, Any]: A dictionary of the estimated values.
        """
