from ray.rllib.offline.estimators.off_policy_estimator import (
    OffPolicyEstimator,
    OffPolicyEstimate,
)
from ray.rllib.utils.annotations import override, ExperimentalAPI
from ray.rllib.utils.typing import SampleBatchType
from typing import List
import numpy as np


@ExperimentalAPI
class ImportanceSampling(OffPolicyEstimator):
    """The step-wise IS estimator.

    Step-wise IS estimator described in https://arxiv.org/pdf/1511.03722.pdf,
    https://arxiv.org/pdf/1911.06854.pdf"""

    @override(OffPolicyEstimator)
    def estimate(self, batch: SampleBatchType) -> List[OffPolicyEstimate]:
        self.check_can_estimate_for(batch)
        estimates = []
        for sub_batch in batch.split_by_episode():
            rewards, old_prob = sub_batch["rewards"], sub_batch["action_prob"]
            new_prob = np.exp(self.action_log_likelihood(sub_batch))

            # calculate importance ratios
            p = []
            for t in range(sub_batch.count):
                if t == 0:
                    pt_prev = 1.0
                else:
                    pt_prev = p[t - 1]
                p.append(pt_prev * new_prob[t] / old_prob[t])

            # calculate stepwise IS estimate
            v_old = 0.0
            v_new = 0.0
            for t in range(sub_batch.count):
                v_old += rewards[t] * self.gamma ** t
                v_new += p[t] * rewards[t] * self.gamma ** t

            estimates.append(
                OffPolicyEstimate(
                    self.name,
                    {
                        "v_old": v_old,
                        "v_new": v_new,
                        "v_gain": v_new / max(1e-8, v_old),
                    },
                )
            )
        return estimates
