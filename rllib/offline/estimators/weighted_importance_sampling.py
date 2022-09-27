from typing import Dict
import numpy as np

from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.policy import compute_log_likelihoods_from_input_dict
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.numpy import convert_to_numpy


@DeveloperAPI
class WeightedImportanceSampling(OffPolicyEstimator):
    """The step-wise WIS estimator.

    Let s_t, a_t, and r_t be the state, action, and reward at timestep t.

    For behavior policy \pi_b and evaluation policy \pi_e, define the
    cumulative importance ratio at timestep t as:
    p_t = \sum_{t'=0}^t (\pi_e(a_{t'} | s_{t'}) / \pi_b(a_{t'} | s_{t'})).

    Define the average importance ratio over episodes i in the dataset D as:
    w_t = \sum_{i \in D} p^(i)_t / |D|

    This estimator computes the expected return for \pi_e for an episode as:
    V^{\pi_e}(s_0) = \E[\sum_t \gamma ^ {t} * (p_t / w_t) * r_t]
    and returns the mean and standard deviation over episodes.

    For more information refer to https://arxiv.org/pdf/1911.06854.pdf"""

    @override(OffPolicyEstimator)
    def __init__(self, policy: Policy, gamma: float):
        super().__init__(policy, gamma)
        self.filter_values = []
        self.filter_counts = []

    @override(OffPolicyEstimator)
    def estimate_multi_step(self, episode: SampleBatch) -> Dict[str, float]:
        estimates_per_epsiode = {"v_behavior": None, "v_target": None}
        rewards, old_prob = episode["rewards"], episode["action_prob"]
        log_likelihoods = compute_log_likelihoods_from_input_dict(self.policy, episode)
        new_prob = np.exp(convert_to_numpy(log_likelihoods))

        # calculate importance ratios
        p = []
        for t in range(episode.count):
            if t == 0:
                pt_prev = 1.0
            else:
                pt_prev = p[t - 1]
            p.append(pt_prev * new_prob[t] / old_prob[t])
        for t, v in enumerate(p):
            if t >= len(self.filter_values):
                self.filter_values.append(v)
                self.filter_counts.append(1.0)
            else:
                self.filter_values[t] += v
                self.filter_counts[t] += 1.0

        # calculate stepwise weighted IS estimate
        v_behavior = 0.0
        v_target = 0.0

        for t in range(episode.count):
            v_behavior += rewards[t] * self.gamma ** t
            w_t = self.filter_values[t] / self.filter_counts[t]
            v_target += p[t] / w_t * rewards[t] * self.gamma ** t

        estimates_per_epsiode["v_behavior"] = v_behavior
        estimates_per_epsiode["v_target"] = v_target

        return estimates_per_epsiode

    @override(OffPolicyEstimator)
    def estimate_single_step(self, batch: SampleBatch) -> Dict[str, float]:
        estimates_per_epsiode = {"v_behavior": None, "v_target": None}
        rewards, old_prob = batch["rewards"], batch["action_prob"]
        log_likelihoods = compute_log_likelihoods_from_input_dict(self.policy, batch)
        new_prob = np.exp(convert_to_numpy(log_likelihoods))

        weights = new_prob / old_prob
        v_behavior = np.mean(rewards)
        v_target = np.mean(weights * rewards) / np.mean(weights)

        estimates_per_epsiode["v_behavior"] = v_behavior
        estimates_per_epsiode["v_target"] = v_target

        return estimates_per_epsiode
