"""Health monitoring and diagnostics for Ray Train.

    import ray.train.health as health

    def train_func(config):
        ...
        health.report({"step_time_s": dt}, step=step)

    trainer = TorchTrainer(
        train_func,
        run_config=ray.train.RunConfig(
            health_config=health.HealthConfig(
                policies=[
                    health.HealthPolicy(
                        probe_creator=lambda: [MyProbe()],
                        evaluator_creator=lambda: [MyEvaluator()],
                    )
                ],
            ),
        ),
    )
"""
from ray.train.v2._internal.constants import is_v2_enabled

if is_v2_enabled():
    from ray.train.health.decision import (
        Action,
        Cause,
        Diagnose,
        Evict,
        HealthDecision,
        Noop,
        Reattempt,
    )
    from ray.train.health.exceptions import HealthDecisionError
    from ray.train.health.policy import (
        Evaluator,
        HealthConfig,
        HealthPolicy,
    )
    from ray.train.health.probe import (
        NODE_SCOPE,
        WORKER_SCOPE,
        ClusterContext,
        ClusterProbe,
        NodeContext,
        NodeInfo,
        NodeProbe,
        OnDemandProbe,
        OnDemandProbeContext,
        Probe,
        ProbeResult,
        WorkerProbe,
    )
    from ray.train.health.report import report
    from ray.train.health.state import HealthState, NodeHealth, WorkerHealth

    __all__ = [
        "Action",
        "Cause",
        "ClusterContext",
        "ClusterProbe",
        "Diagnose",
        "Evaluator",
        "Evict",
        "HealthConfig",
        "HealthDecision",
        "HealthDecisionError",
        "HealthPolicy",
        "HealthState",
        "NODE_SCOPE",
        "NodeContext",
        "NodeHealth",
        "NodeInfo",
        "NodeProbe",
        "Noop",
        "OnDemandProbe",
        "OnDemandProbeContext",
        "Probe",
        "ProbeResult",
        "Reattempt",
        "WORKER_SCOPE",
        "WorkerHealth",
        "WorkerProbe",
        "report",
    ]
else:
    raise ImportError(
        "`ray.train.health` is only available in Ray Train v2. "
        "To enable it, please set `RAY_TRAIN_V2_ENABLED=1`."
    )

# DO NOT ADD ANYTHING AFTER THIS LINE.
