from collections import namedtuple
import logging

import numpy as np

from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.offline.io_context import IOContext
from ray.rllib.utils.annotations import Deprecated
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import TensorType, SampleBatchType
from typing import List

logger = logging.getLogger(__name__)

OffPolicyEstimate = namedtuple("OffPolicyEstimate", ["estimator_name", "metrics"])


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
        self.new_estimates = []

    @classmethod
    def create_from_io_context(cls, ioctx: IOContext) -> "OffPolicyEstimator":
        """Creates an off-policy estimator from an IOContext object.

        Extracts Policy and gamma (discount factor) information from the
        IOContext.

        Args:
            ioctx: The IOContext object to create the OffPolicyEstimator
                from.

        Returns:
            The OffPolicyEstimator object created from the IOContext object.
        """
        gamma = ioctx.worker.policy_config["gamma"]
        # Grab a reference to the current model
        keys = list(ioctx.worker.policy_map.keys())
        if len(keys) > 1:
            raise NotImplementedError(
                "Off-policy estimation is not implemented for multi-agent. "
                "You can set `input_evaluation: []` to resolve this."
            )
        policy = ioctx.worker.get_policy(keys[0])
        return cls(policy, gamma)

    @DeveloperAPI
    def estimate(self, batch: SampleBatchType) -> OffPolicyEstimate:
        """Returns an off policy estimate for the given batch of experiences.

        The batch will at most only contain data from one episode,
        but it may also only be a fragment of an episode.

        Args:
            batch: The batch to calculate the off policy estimate (OPE) on.

        Returns:
            The off-policy estimates (OPE) calculated on the given batch.
        """
        raise NotImplementedError

    @DeveloperAPI
    def action_log_likelihood(self, batch: SampleBatchType) -> TensorType:
        """Returns log likelihoods for actions in given batch for policy.

        Computes likelihoods by passing the observations through the current
        policy's `compute_log_likelihoods()` method.

        Args:
            batch: The SampleBatch or MultiAgentBatch to calculate action
                log likelihoods from. This batch/batches must contain OBS
                and ACTIONS keys.

        Returns:
            The log likelihoods of the actions in the batch, given the
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
        return np.exp(log_likelihoods)

    @DeveloperAPI
    def process(self, batch: SampleBatchType) -> None:
        """Computes off policy estimates (OPE) on batch and stores results.

        Thus-far collected results can be retrieved then by calling
        `self.get_metrics` (which flushes the internal results storage).

        Args:
            batch: The batch to process (call `self.estimate()` on) and
                store results (OPEs) for.
        """
        self.new_estimates.append(self.estimate(batch))

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
                "IS-estimation is not implemented for multi-agent batches. "
                "You can set `input_evaluation: []` to resolve this."
            )

        if "action_prob" not in batch:
            raise ValueError(
                "Off-policy estimation is not possible unless the inputs "
                "include action probabilities (i.e., the policy is stochastic "
                "and emits the 'action_prob' key). For DQN this means using "
                "`exploration_config: {type: 'SoftQ'}`. You can also set "
                "`input_evaluation: []` to disable estimation."
            )

    @DeveloperAPI
    def get_metrics(self) -> List[OffPolicyEstimate]:
        """Returns list of new episode metric estimates since the last call.

        Returns:
            List of OffPolicyEstimate objects.
        """
        out = self.new_estimates
        self.new_estimates = []
        return out

    @Deprecated(new="OffPolicyEstimator.create_from_io_context", error=False)
    def create(self, *args, **kwargs):
        return self.create_from_io_context(*args, **kwargs)

    @Deprecated(new="OffPolicyEstimator.action_log_likelihood", error=False)
    def action_prob(self, *args, **kwargs):
        return self.action_log_likelihood(*args, **kwargs)
