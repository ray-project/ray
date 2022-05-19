from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimate
from ray.rllib.policy import Policy
from ray.rllib.offline.estimators.direct_method import DirectMethod
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import SampleBatchType


class DoublyRobust(DirectMethod):
    """The Doubly Robust (DR) estimator with FQE Q-function.

    DR estimator described in https://arxiv.org/pdf/1511.03722.pdf,
    FQE-DR in https://arxiv.org/pdf/1911.06854.pdf"""

    @override(DirectMethod)
    def estimate(self, batch: SampleBatchType) -> OffPolicyEstimate:
        self.check_can_estimate_for(batch)
        estimates = []

        return estimates