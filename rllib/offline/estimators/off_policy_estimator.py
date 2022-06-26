import logging
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import TensorType, SampleBatchType
from typing import List, Dict

logger = logging.getLogger(__name__)


@DeveloperAPI
class OffPolicyEstimator:
    """Interface for an off policy reward estimator."""

    @DeveloperAPI
    def __init__(self, config: Dict):
        """Initializes an OffPolicyEstimator instance.

        Args:
            name: string to save OPE results under
            policy: Policy to evaluate.
            gamma: Discount factor of the environment.
        """
        self.name = config["name"]
        self.policy = config["policy"]
        self.gamma = config["gamma"]

    @DeveloperAPI
    def estimate(self, batch: SampleBatchType) -> Dict[str, List]:
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
        """Returns log likelihood for actions in given batch for policy.

        Computes likelihoods by passing the observations through the current
        policy's `compute_log_likelihoods()` method

        Args:
            batch: The SampleBatch or MultiAgentBatch to calculate action
                log likelihoods from. This batch/batches must contain OBS
                and ACTIONS keys.

        Returns:
            The probabilities of the actions in the batch, given the
            observations and the policy.
        """
        num_state_inputs = 0
        for k in batch.keys():
            if k.startswith("state_in_"):
                num_state_inputs += 1
        state_keys = ["state_in_{}".format(i) for i in range(num_state_inputs)]
        log_likelihoods: TensorType = self.policy.compute_log_likelihoods(
            actions=batch[SampleBatch.ACTIONS],
            obs_batch=batch[SampleBatch.OBS],
            state_batches=[batch[k] for k in state_keys],
            prev_action_batch=batch.get(SampleBatch.PREV_ACTIONS),
            prev_reward_batch=batch.get(SampleBatch.PREV_REWARDS),
            actions_normalized=True,
        )
        log_likelihoods = convert_to_numpy(log_likelihoods)
        return log_likelihoods

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
            raise ValueError(
                "Off-Policy Estimation is not implemented for multi-agent batches. "
                "You can set `off_policy_estimation_methods: {}` to resolve this."
            )

        if "action_prob" not in batch:
            raise ValueError(
                "Off-policy estimation is not possible unless the inputs "
                "include action probabilities (i.e., the policy is stochastic "
                "and emits the 'action_prob' key). For DQN this means using "
                "`exploration_config: {type: 'SoftQ'}`. You can also set "
                "`off_policy_estimation_methods: {}` to disable estimation."
            )
