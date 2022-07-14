import logging
from typing import Dict, Any
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import SampleBatchType
import numpy as np
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.policy import compute_log_likelihoods_from_input_dict

from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator

torch, nn = try_import_torch()

logger = logging.getLogger()


@DeveloperAPI
class DoublyRobust(OffPolicyEstimator):
    """The Doubly Robust estimator.

    DR estimator described in https://arxiv.org/pdf/1511.03722.pdf"""

    @override(OffPolicyEstimator)
    def __init__(
        self,
        policy: Policy,
        gamma: float,
        q_model_config: Dict = None,
    ):
        """Initializes a Doubly Robust OPE Estimator.

        Args:
            policy: Policy to evaluate.
            gamma: Discount factor of the environment.
            q_model_config: Arguments to specify the Q-model. Must specify
            a `type` key pointing to the Q-model class.
            This Q-model is trained in the train() method and is used
            to compute the state-value and Q-value estimates
            for the DoublyRobust estimator.
            It must implement `train`, `estimate_q`, and `estimate_v`.
            TODO (Rohan138): Unify this with RLModule API.
        """

        super().__init__(policy, gamma)
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
        """Compute off-policy estimates.

        Args:
            batch: The SampleBatch to run off-policy estimation on

        Returns:
            A dict consists of the following metrics:
            - v_behavior: The discounted return averaged over episodes in the batch
            - v_behavior_std: The standard deviation corresponding to v_behavior
            - v_target: The estimated discounted return for `self.policy`,
            averaged over episodes in the batch
            - v_target_std: The standard deviation corresponding to v_target
            - v_gain: v_target / max(v_behavior, 1e-8), averaged over episodes
            - v_gain_std: The standard deviation corresponding to v_gain
        """
        self.convert_ma_batch_to_sample_batch(batch)
        self.check_action_prob_in_batch(batch)
        estimates = {"v_behavior": [], "v_target": [], "v_gain": []}
        # Calculate doubly robust OPE estimates
        for episode in batch.split_by_episode():
            rewards, old_prob = episode["rewards"], episode["action_prob"]
            log_likelihoods = compute_log_likelihoods_from_input_dict(
                self.policy, episode
            )
            new_prob = np.exp(convert_to_numpy(log_likelihoods))

            v_behavior = 0.0
            v_target = 0.0
            q_values = self.model.estimate_q(episode)
            q_values = convert_to_numpy(q_values)
            v_values = self.model.estimate_v(episode)
            v_values = convert_to_numpy(v_values)
            assert q_values.shape == v_values.shape == (episode.count,)

            for t in reversed(range(episode.count)):
                v_behavior = rewards[t] + self.gamma * v_behavior
                v_target = v_values[t] + (new_prob[t] / old_prob[t]) * (
                    rewards[t] + self.gamma * v_target - q_values[t]
                )
            v_target = v_target.item()

            estimates["v_behavior"].append(v_behavior)
            estimates["v_target"].append(v_target)
            estimates["v_gain"].append(v_target / max(v_behavior, 1e-8))
        estimates["v_behavior_std"] = np.std(estimates["v_behavior"])
        estimates["v_behavior"] = np.mean(estimates["v_behavior"])
        estimates["v_target_std"] = np.std(estimates["v_target"])
        estimates["v_target"] = np.mean(estimates["v_target"])
        estimates["v_gain_std"] = np.std(estimates["v_gain"])
        estimates["v_gain"] = np.mean(estimates["v_gain"])
        return estimates

    @override(OffPolicyEstimator)
    def train(self, batch: SampleBatchType) -> Dict[str, Any]:
        """Trains self.model on the given batch.

        Args:
        batch: A SampleBatch or MultiAgentbatch to train on

        Returns:
        A dict with key "loss" and value as the mean training loss.
        """
        self.convert_ma_batch_to_sample_batch(batch)
        losses = self.model.train(batch)
        return {"loss": np.mean(losses)}
