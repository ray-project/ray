import logging
from ray.rllib.policy.sample_batch import (
    MultiAgentBatch,
    DEFAULT_POLICY_ID,
    SampleBatch,
)
from ray.rllib.policy import Policy
from ray.rllib.utils.policy import compute_log_likelihoods_from_input_dict
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import TensorType, SampleBatchType
from typing import Dict, Any

logger = logging.getLogger(__name__)


@DeveloperAPI
class OffPolicyEstimator:
    """Interface for an off policy reward estimator."""

    @DeveloperAPI
    def __init__(self, policy: Policy, gamma: float):
        """Initializes an OffPolicyEstimator instance.

        Args:
            policy: Policy to evaluate.
            gamma: Discount factor of the environment.
        """
        self.policy = policy
        self.gamma = gamma

    @DeveloperAPI
    def estimate(self, batch: SampleBatchType) -> Dict[str, Any]:
        """Returns off-policy estimates for the given batch of episodes.

        Args:
            batch: The batch to calculate the off-policy estimates (OPE) on. The
            batch must contain the fields "obs", "actions", and "action_prob".

        Returns:
            The off-policy estimates (OPE) calculated on the given batch. The returned
            dict can be any arbitrary mapping of strings to metrics.
        """
        raise NotImplementedError

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

    @DeveloperAPI
    def check_action_prob_in_batch(self, batch: SampleBatchType) -> None:
        """Checks if we support off policy estimation (OPE) on given batch.

        Args:
            batch: The batch to check.

        Raises:
            ValueError: In case `action_prob` key is not in batch
        """

        if "action_prob" not in batch:
            raise ValueError(
                "Off-policy estimation is not possible unless the inputs "
                "include action probabilities (i.e., the policy is stochastic "
                "and emits the 'action_prob' key). For DQN this means using "
                "`exploration_config: {type: 'SoftQ'}`. You can also set "
                "`off_policy_estimation_methods: {}` to disable estimation."
            )

    @DeveloperAPI
    def train(self, batch: SampleBatchType) -> Dict[str, Any]:
        """Train a model for Off-Policy Estimation.

        Args:
            batch: SampleBatch to train on

        Returns:
            Any optional metrics to return from the estimator
        """
        return {}

    @Deprecated(
        old="OffPolicyEstimator.action_log_likelihood",
        new="ray.rllib.utils.policy.compute_log_likelihoods_from_input_dict",
        error=False,
    )
    def action_log_likelihood(self, batch: SampleBatchType) -> TensorType:
        log_likelihoods = compute_log_likelihoods_from_input_dict(self.policy, batch)
        return convert_to_numpy(log_likelihoods)
