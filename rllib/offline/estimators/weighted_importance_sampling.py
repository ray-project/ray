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
    """The weighted step-wise IS estimator.

    Step-wise WIS estimator in https://arxiv.org/pdf/1511.03722.pdf,
    https://arxiv.org/pdf/1911.06854.pdf"""

    @override(OffPolicyEstimator)
    def __init__(self, name: str, policy: Policy, gamma: float):
        super().__init__(name, policy, gamma)
        self.filter_values = []
        self.filter_counts = []

    @override(OffPolicyEstimator)
    def estimate(self, batch: SampleBatchType) -> Dict[str, Any]:
        self.check_can_estimate_for(batch)
        estimates = {"v_old": [], "v_new": [], "v_gain": []}
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
            v_old = 0.0
            v_new = 0.0
            for t in range(episode.count):
                v_old += rewards[t] * self.gamma ** t
                w_t = self.filter_values[t] / self.filter_counts[t]
                v_new += p[t] / w_t * rewards[t] * self.gamma ** t

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
