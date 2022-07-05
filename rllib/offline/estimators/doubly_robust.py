import logging
from typing import Dict, Any
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import MultiAgentBatch, DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import SampleBatchType
import numpy as np
from ray.rllib.utils.numpy import convert_to_numpy

from ray.rllib.offline.estimators.off_policy_estimator import (
    OffPolicyEstimator,
    action_log_likelihood,
)

torch, nn = try_import_torch()

logger = logging.getLogger()


@DeveloperAPI
class DoublyRobust(OffPolicyEstimator):
    """The Doubly Robust estimator with a trainable Q-model.

    DR estimator described in https://arxiv.org/pdf/1511.03722.pdf"""

    @override(OffPolicyEstimator)
    def __init__(
        self,
        name: str,
        policy: Policy,
        gamma: float,
        q_model_config: Dict = None,
    ):
        """
        Initializes a Direct Method OPE Estimator.

        Args:
            name: string to save OPE results under
            policy: Policy to evaluate.
            gamma: Discount factor of the environment.
            q_model_config: Arguments to specify the Q-model.
        """

        super().__init__(name, policy, gamma)
        model_cls = q_model_config.pop("type")

        self.model = model_cls(
            policy=policy,
            gamma=gamma,
            **q_model_config,
        )
        assert hasattr(
            self.model, "estimate_v"
        ), "self.model must implement `estimate_v`!"
        assert hasattr(
            self.model, "estimate_q"
        ), "self.model must implement `estimate_q`!"

    @override(OffPolicyEstimator)
    def estimate(self, batch: SampleBatchType) -> Dict[str, Any]:
        self.check_can_estimate_for(batch)
        estimates = {"v_old": [], "v_new": [], "v_gain": []}
        # Calculate doubly robust OPE estimates
        for episode in batch.split_by_episode():
            rewards, old_prob = episode["rewards"], episode["action_prob"]
            new_prob = np.exp(action_log_likelihood(self.policy, episode))

            v_old = 0.0
            v_new = 0.0
            q_values = self.model.estimate_q(episode)
            q_values = convert_to_numpy(q_values)
            v_values = self.model.estimate_v(episode)
            v_values = convert_to_numpy(v_values)
            assert q_values.shape == v_values.shape == (episode.count,)

            for t in reversed(range(episode.count)):
                v_old = rewards[t] + self.gamma * v_old
                v_new = v_values[t] + (new_prob[t] / old_prob[t]) * (
                    rewards[t] + self.gamma * v_new - q_values[t]
                )
            v_new = v_new.item()

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

    @override(OffPolicyEstimator)
    def train(self, batch: SampleBatchType) -> Dict[str, Any]:
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
        losses = self.model.train(batch)
        return {"loss": np.mean(losses)}
