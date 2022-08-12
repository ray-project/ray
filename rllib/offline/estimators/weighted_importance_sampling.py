from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator
from ray.rllib.utils.policy import compute_log_likelihoods_from_input_dict
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import SampleBatchType
import numpy as np
from typing import Dict, Any


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
        batch = self.convert_ma_batch_to_sample_batch(batch)
        self.check_action_prob_in_batch(batch)
        estimates = {"v_behavior": [], "v_target": [], "v_gain": []}
        for episode in batch.split_by_episode():
            rewards, old_prob = episode["rewards"], episode["action_prob"]
            log_likelihoods = compute_log_likelihoods_from_input_dict(
                self.policy, episode
            )
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
