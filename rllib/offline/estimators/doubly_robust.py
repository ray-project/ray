from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimate
from ray.rllib.offline.estimators.direct_method import DirectMethod
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import SampleBatchType
import numpy as np


class DoublyRobust(DirectMethod):
    """The Doubly Robust (DR) estimator with FQE Q-function.

    DR estimator described in https://arxiv.org/pdf/1511.03722.pdf,
    FQE-DR in https://arxiv.org/pdf/1911.06854.pdf"""

    @override(DirectMethod)
    def estimate(self, batch: SampleBatchType) -> OffPolicyEstimate:
        self.check_can_estimate_for(batch)
        estimates = []
        for episode in batch.split_by_episode():
            rewards, old_prob = episode["rewards"], episode["action_prob"]
            new_prob = self.action_prob(episode)

            # calculate importance ratios
            p = []
            for t in range(episode.count):
                if t == 0:
                    pt_prev = 1.0
                else:
                    pt_prev = p[t - 1]
                p.append(pt_prev * new_prob[t] / old_prob[t])

            V_prev, V_DR = 0.0, 0.0
            for t in range(episode.count):
                V_prev += rewards[t] * self.gamma ** t

            estimates.append(OffPolicyEstimate(
                "doubly_robust",
                {
                    "V_prev": V_prev,
                    "V_DR": V_DR,
                    "V_gain_est": V_DR / max(1e-8, V_prev),
                },
            ))
        return estimates