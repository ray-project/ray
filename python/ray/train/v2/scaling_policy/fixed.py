from ray.train.v2.scaling_policy.scaling_policy import ScalingDecision, ScalingPolicy


class FixedScalingPolicy(ScalingPolicy):
    def supports_elasticity(self) -> bool:
        return False

    def make_decision(self, worker_group_status) -> ScalingDecision:
        return ScalingDecision()
