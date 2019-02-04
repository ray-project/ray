from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils.annotations import override


class ImportanceSamplingEstimator(OffPolicyEstimator):
    """The step-wise IS estimator.

    Step-wise IS estimator described in https://arxiv.org/pdf/1511.03722.pdf"""

    def __init__(self, ioctx):
        OffPolicyEstimator.__init__(self, ioctx)

    @override(OffPolicyEstimator)
    def process(self, batch):
        if not self.can_estimate_for(batch):
            return

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
                "V_gain_est": V_step_IS / V_prev,
            })
        self.estimates.append(estimation)
