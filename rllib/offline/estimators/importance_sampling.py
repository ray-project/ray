from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator, OffPolicyEstimate
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import SampleBatchType
from typing import List


class ImportanceSampling(OffPolicyEstimator):
    """The step-wise IS estimator.

    Step-wise IS estimator described in https://arxiv.org/pdf/1511.03722.pdf,
    https://arxiv.org/pdf/1911.06854.pdf"""

    @override(OffPolicyEstimator)
    def estimate(self, batch: SampleBatchType) -> List[OffPolicyEstimate]:
        self.check_can_estimate_for(batch)
        estimates = []
        # TODO (rohan) : Optimize this to use matmul instead of for loop
        for sub_batch in batch.split_by_episode():
            rewards, old_prob = sub_batch["rewards"], sub_batch["action_prob"]
            new_prob = self.action_prob(sub_batch)

            # calculate importance ratios
            p = []
            for t in range(sub_batch.count):
                if t == 0:
                    pt_prev = 1.0
                else:
                    pt_prev = p[t - 1]
                p.append(pt_prev * new_prob[t] / old_prob[t])

            # calculate stepwise IS estimate
            V_prev, V_step_IS = 0.0, 0.0
            for t in range(sub_batch.count):
                V_prev += rewards[t] * self.gamma ** t
                V_step_IS += p[t] * rewards[t] * self.gamma ** t

            estimates.append(OffPolicyEstimate(
                "importance_sampling",
                {
                    "V_prev": V_prev,
                    "V_step_IS": V_step_IS,
                    "V_gain_est": V_step_IS / max(1e-8, V_prev),
                },
            ))
        return estimates
