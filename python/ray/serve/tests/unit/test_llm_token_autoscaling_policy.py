import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ray.llm._internal.serve.routing_policies.kv_aware.autoscaling_policy import (
    LLMTokenAutoscalingPolicy,
)
from ray.serve._private.common import DeploymentID, TimeStampedValue


@pytest.mark.asyncio
async def test_router_report_reaches_app_policy():
    from ray.llm._internal.serve.core.ingress.router import LLMRouter
    from ray.serve._private.autoscaling_state import AutoscalingStateManager
    from ray.serve._private.common import ReplicaID, ReplicaMetricReport
    from ray.serve._private.config import AutoscalingMetricsConfig

    target = DeploymentID(name="decode", app_name="app")
    source = DeploymentID(name="LLMRouter", app_name="app")
    replica = ReplicaID("router-0", source)
    now = time.time()
    tracker = AsyncMock()
    tracker.get_autoscaling_snapshot.return_value = {
        "outstanding_effective_prefill_tokens": 21
    }
    router = SimpleNamespace(
        _kv_token_tracker=tracker, _handle=SimpleNamespace(deployment_id=target)
    )
    sample = await LLMRouter.record_autoscaling_stats(router)
    manager = AutoscalingStateManager()
    manager.configure_metrics_source(source, AutoscalingMetricsConfig())
    manager.update_running_replica_ids(source, [replica])
    manager.record_request_metrics_for_replica(
        ReplicaMetricReport(
            replica, {k: [TimeStampedValue(now - 1, v)] for k, v in sample.items()}, now
        )
    )
    app = manager._app_autoscaling_states["app"]
    app._policy = LLMTokenAutoscalingPolicy(10)
    server = Mock()
    server.get_autoscaling_context.return_value = SimpleNamespace(
        app_name="app",
        current_time=now,
        total_num_requests=0,
        config=SimpleNamespace(look_back_period_s=30),
        raw_metrics={},
    )
    app._deployment_autoscaling_states[target] = server
    assert app.get_decision_num_replicas(
        {source: 1, target: 0}, _skip_bound_check=True
    ) == {target: 3}
    assert not manager.should_autoscale_deployment(source)
    manager.update_running_replica_ids(source, [])
    assert app.get_decision_num_replicas({source: 1, target: 3}) == {}

    from ray.serve.autoscaling_policy import _apply_app_level_autoscaling_config

    app._policy = _apply_app_level_autoscaling_config(
        lambda contexts: ({source: 2}, {})
    )
    with pytest.raises(ValueError, match="Cannot scale metrics-only deployment"):
        app.get_decision_num_replicas({source: 1})


@pytest.mark.parametrize("requests,load,expected", [(0, 20, 2), (1, 0, 1), (0, 0, 0)])
def test_replicated_router_reports(requests, load, expected):
    server = DeploymentID(name="decode", app_name="app")
    router = DeploymentID(name="LLMRouter", app_name="app")
    points = [TimeStampedValue(10, load)]
    key = "outstanding_effective_prefill_tokens:decode"
    metrics = {key: {"router1": points, "router2": points}}
    ctx = SimpleNamespace(
        app_name="app",
        config=SimpleNamespace(look_back_period_s=10),
        current_time=20,
        total_num_requests=requests,
        raw_metrics={},
    )
    contexts = {server: ctx, router: SimpleNamespace(config=None, raw_metrics=metrics)}
    policy = LLMTokenAutoscalingPolicy(10)
    assert policy(contexts) == ({server: expected}, {})
    metrics["outstanding_effective_prefill_tokens:another"] = metrics[key]
    assert policy(contexts) == ({}, {})
    del contexts[router]
    assert policy(contexts) == ({}, {})
