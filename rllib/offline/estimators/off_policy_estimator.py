import logging
from ray.rllib.policy.sample_batch import MultiAgentBatch, DEFAULT_POLICY_ID
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import TensorType, SampleBatchType
from ray.rllib.offline.estimators.utils import action_log_likelihood
from typing import Dict, Any

logger = logging.getLogger(__name__)


@DeveloperAPI
class OffPolicyEstimator:
    """Interface for an off policy reward estimator."""

    @DeveloperAPI
    def __init__(self, name: str, policy: Policy, gamma: float):
        """Initializes an OffPolicyEstimator instance.

        Args:
            name: string to save OPE results under
            policy: Policy to evaluate.
            gamma: Discount factor of the environment.
        """
        self.name = name
        self.policy = policy
        self.gamma = gamma

    @DeveloperAPI
    def estimate(self, batch: SampleBatchType) -> Dict[str, Any]:
        """Returns off policy estimates for the given batch of episodes.

        Args:
            batch: The batch to calculate the off policy estimates (OPE) on.

        Returns:
            The off-policy estimates (OPE) calculated on the given batch. Note
            that this dictionary can contain arbitrary strings mapping to the
            list of metrics e.g. {"v_old": [1, 2, 3], "v_new": [4,5,6]}
        """
        raise NotImplementedError

    @DeveloperAPI
    def action_log_likelihood(self, batch: SampleBatchType) -> TensorType:
        # TODO (Rohan138): Add deprecation warning
        return action_log_likelihood(self.policy, batch)

    @DeveloperAPI
    def check_can_estimate_for(self, batch: SampleBatchType) -> None:
        """Checks if we support off policy estimation (OPE) on given batch.

        Args:
            batch: The batch to check.

        Raises:
            ValueError: In case `action_prob` key is not in batch OR batch
            is a MultiAgentBatch.
        """

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
