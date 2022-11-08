import abc
import os
import logging
from typing import Dict, Any, Union, Optional

from ray.air.checkpoint import Checkpoint
from ray.data import Dataset

from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import DeveloperAPI
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

    @abstractmethod
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

    @abstractclassmethod
    def estimate_on_dataset(
        cls, 
        dataset: Dataset, 
        checkpoint: Union[str, Checkpoint] = None,
        policy_state: Optional[Dict[str, Any]] = None,
        *, 
        n_parallelism: int = os.cpu_count(),
    ) -> Dict[str, Any]:

        """Calculates the estmiate of the metrics based on the given offline dataset. 

        Args:
            dataset: The ray dataset object to do offline evaluation on.
            checkpoint: The checkpoint to load the policy from. Only one of checkpoint 
                and policy_state can be provided.
            policy_state: The state of the policy to use for the evaluation. Only one 
                of checkpoint and policy_state can be provided.
            n_parallelism: The number of parallelism to use for the computation.

        Returns:
            Dict[str, Any]: A dictionary of the estimated values.
        """