from ray.rllib.offline.off_policy_estimator import OffPolicyEstimator, \
    OffPolicyEstimate
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import SampleBatchType


class WeightedImportanceSamplingEstimator(OffPolicyEstimator):
    """The weighted step-wise IS estimator.

    Step-wise WIS estimator in https://arxiv.org/pdf/1511.03722.pdf"""

    def __init__(self, policy: Policy, gamma: float):
        super().__init__(policy, gamma)
        self.filter_values = []
        self.filter_counts = []

    @override(OffPolicyEstimator)
    def estimate(self, batch: SampleBatchType) -> OffPolicyEstimate:
        self.check_can_estimate_for(batch)

        rewards, old_prob = batch["rewards"], batch["action_prob"]
        new_prob = self.action_prob(batch)

        # calculate importance ratios
        p = []
        for t in range(batch.count):
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
        V_prev, V_step_WIS = 0.0, 0.0
        for t in range(batch.count):
            V_prev += rewards[t] * self.gamma**t
            w_t = self.filter_values[t] / self.filter_counts[t]
            V_step_WIS += p[t] / w_t * rewards[t] * self.gamma**t

        estimation = OffPolicyEstimate(
            "wis", {
                "V_prev": V_prev,
                "V_step_WIS": V_step_WIS,
                "V_gain_est": V_step_WIS / max(1e-8, V_prev),
            })
        return estimation
