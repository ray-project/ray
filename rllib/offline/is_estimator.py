from ray.rllib.offline.off_policy_estimator import OffPolicyEstimator, \
    OffPolicyEstimate
from ray.rllib.utils.annotations import override


class ImportanceSamplingEstimator(OffPolicyEstimator):
    """The step-wise IS estimator.

    Step-wise IS estimator described in https://arxiv.org/pdf/1511.03722.pdf"""

    @override(OffPolicyEstimator)
    def estimate(self, batch):
        self.check_can_estimate_for(batch)

        rewards, old_prob = batch["rewards"], batch["action_prob"]
        new_prob = self.action_prob(batch)

        # calculate importance ratios
        p = []
        for t in range(batch.count - 1):
            if t == 0:
                pt_prev = 1.0
            else:
                pt_prev = p[t - 1]
            p.append(pt_prev * new_prob[t] / old_prob[t])

        # calculate stepwise IS estimate
        V_prev, V_step_IS = 0.0, 0.0
        for t in range(batch.count - 1):
            V_prev += rewards[t] * self.gamma**t
            V_step_IS += p[t] * rewards[t] * self.gamma**t

        estimation = OffPolicyEstimate(
            "is", {
                "V_prev": V_prev,
                "V_step_IS": V_step_IS,
                "V_gain_est": V_step_IS / max(1e-8, V_prev),
            })
        return estimation
