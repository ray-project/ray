import logging
from typing import Dict, Any, Callable
from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator
from ray.rllib.offline.estimators.utils import lookup_state_value_fn
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import SampleBatchType, TensorType
import numpy as np

torch, nn = try_import_torch()

logger = logging.getLogger()


@DeveloperAPI
class DirectMethod(OffPolicyEstimator):
    """The Direct Method estimator.

    DM estimator described in https://arxiv.org/pdf/1511.03722.pdf"""

    @override(OffPolicyEstimator)
    def __init__(
        self,
        name: str,
        policy: Policy,
        gamma: float,
        state_value_fn: Callable[[Policy, SampleBatch], TensorType] = None,
    ):
        """
        Initializes a Direct Method OPE Estimator.

        Args:
            name: string to save OPE results under
            policy: Policy to evaluate.
            gamma: Discount factor of the environment.
            state_value_fn: Function that takes in self.policy and a
            SampleBatch with states s and return the state values V(s).
            This is meant to be generic; modify this for your Algorithm as neccessary.
            If None, try to look up the function using lookup_state_value_fn.
        """

        super().__init__(name, policy, gamma)
        self.state_value_fn = state_value_fn or lookup_state_value_fn(policy)

    @override(OffPolicyEstimator)
    def estimate(self, batch: SampleBatchType) -> Dict[str, Any]:
        self.check_can_estimate_for(batch)
        estimates = {"v_old": [], "v_new": [], "v_gain": []}
        # Split data into train and test batches
        for episode in batch.split_by_episode():
            rewards = episode["rewards"]
            v_old = 0.0
            v_new = 0.0
            for t in range(episode.count):
                v_old += rewards[t] * self.gamma ** t

            init_step = episode[0:1]
            v_new = self.state_value_fn(self.policy, init_step)
            v_new = convert_to_numpy(v_new).item()

            estimates["v_old"].append(v_old)
            estimates["v_new"].append(v_new)
            estimates["v_gain"].append(v_new / max(v_old, 1e-8))
        estimates["v_old_std"] = np.std(estimates["v_old"])
        estimates["v_old"] = np.mean(estimates["v_old"])
        estimates["v_new_std"] = np.std(estimates["v_new"])
        estimates["v_new"] = np.mean(estimates["v_new"])
        estimates["v_gain_std"] = np.std(estimates["v_gain"])
        estimates["v_gain"] = np.mean(estimates["v_gain"])
        return estimates
