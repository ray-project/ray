from ray.train.v2.scaling_policy.scaling_policy import ScalingDecision, ScalingPolicy


class FixedScalingPolicy(ScalingPolicy):
    @classmethod
    def supports_elasticity(cls) -> bool:
        return False

    def make_decision(self, worker_group_status) -> ScalingDecision:
        return ScalingDecision()
