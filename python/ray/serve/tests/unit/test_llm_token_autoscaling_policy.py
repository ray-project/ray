import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ray.llm._internal.serve.routing_policies.kv_aware.autoscaling_policy import (
    LLMTokenAutoscalingPolicy,
)
from ray.serve._private.common import DeploymentID, TimeStampedValue
from ray.serve.autoscaling_policy import _apply_app_level_autoscaling_config
from ray.serve.config import (
    AutoscalingConfig,
    AutoscalingContext,
    AutoscalingMetricsContext,
)


def _server_context(dep, now, requests):
    return AutoscalingContext(
        deployment_id=dep,
        deployment_name=dep.name,
        app_name=dep.app_name,
        current_time=now,
        total_num_requests=requests,
        total_queued_requests=0,
        target_num_replicas=0,
        current_num_replicas=0,
        running_replicas=[],
        capacity_adjusted_min_replicas=0,
        capacity_adjusted_max_replicas=8,
        config=AutoscalingConfig(min_replicas=0, max_replicas=8),
        policy_state={},
        last_scale_up_time=None,
        last_scale_down_time=None,
        raw_metrics={},
        aggregated_metrics={},
    )


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
    app._policy = _apply_app_level_autoscaling_config(LLMTokenAutoscalingPolicy(10))
    server = Mock()
    server.get_autoscaling_context.return_value = _server_context(target, now, 0)
    app._deployment_autoscaling_states[target] = server
    assert app.get_decision_num_replicas(
        {source: 1, target: 0}, _skip_bound_check=True
    ) == {target: 3}
    assert not manager.should_autoscale_deployment(source)
    manager.update_running_replica_ids(source, [])
    assert app.get_decision_num_replicas({source: 1, target: 3}) == {}

    app._policy = _apply_app_level_autoscaling_config(
        lambda contexts: ({source: 2}, {})
    )
    with pytest.raises(ValueError, match="Cannot scale metrics-only deployment"):
        app.get_decision_num_replicas({source: 1})


@pytest.mark.parametrize("requests,load,expected", [(0, 20, 2), (1, 0, 1), (0, 0, 0)])
def test_replicated_router_reports(requests, load, expected):
    server = DeploymentID(name="decode", app_name="app")
    router = DeploymentID(name="LLMRouter", app_name="app")
    key = "outstanding_effective_prefill_tokens:decode"
    metrics = {key: {"router1": load, "router2": load}}
    contexts = {
        server: _server_context(server, 20, requests),
        router: AutoscalingMetricsContext(
            router,
            router.name,
            router.app_name,
            2,
            [],
            20,
            aggregated_metrics=metrics,
            raw_metrics={},
        ),
    }
    policy = LLMTokenAutoscalingPolicy(10)
    assert policy(contexts) == ({server: expected}, {})
    metrics["outstanding_effective_prefill_tokens:another"] = metrics[key]
    assert policy(contexts) == ({}, {})
    del contexts[router]
    assert policy(contexts) == ({}, {})


def _cooldown_contexts(now, tokens, requests=0):
    target = DeploymentID("decode", "app")
    source = DeploymentID("LLMRouter", "app")
    server = _server_context(target, now, requests)
    server.target_num_replicas = 3
    server.current_num_replicas = 4
    server.last_scale_down_time = 100
    router = AutoscalingMetricsContext(
        source,
        source.name,
        source.app_name,
        1,
        [],
        now,
        aggregated_metrics={
            "outstanding_effective_prefill_tokens:decode": {"r": tokens}
        },
        raw_metrics={},
    )
    return target, {source: router, target: server}


@pytest.mark.parametrize("tokens,required", [(10, 1), (50, 5)])
def test_cooldown_holds_server_and_restarts_delays(monkeypatch, tokens, required):
    target, contexts = _cooldown_contexts(150, tokens)
    server = contexts[target]
    server.config = AutoscalingConfig(
        min_replicas=0,
        max_replicas=8,
        upscale_delay_s=10,
        downscale_delay_s=10,
        downscale_to_zero_delay_s=10,
    )
    policy = _apply_app_level_autoscaling_config(LLMTokenAutoscalingPolicy(10, 60))
    for now in [150, 159, 160, 169, 170]:
        monkeypatch.setattr("ray.serve.autoscaling_policy.time.time", lambda: now)
        server.current_time = now
        decisions, states = policy(contexts)
        assert decisions == {target: 3 if now < 170 else required}
        server.policy_state = states[target]
        assert server.policy_state["required_replicas"] == required


def test_cooldown_inflight_floor_and_factor_validation():
    target, contexts = _cooldown_contexts(150, 0, requests=1)
    server = contexts[target]
    server.target_num_replicas = 0
    server.current_num_replicas = 0
    policy = LLMTokenAutoscalingPolicy(10, 60)
    assert policy(contexts)[0] == {target: 1}
    server.config = AutoscalingConfig(upscaling_factor=2)
    with pytest.raises(ValueError, match="factors of 1"):
        policy(contexts)


@pytest.mark.parametrize("duration", [-1, float("inf"), float("nan")])
def test_invalid_cooldown_duration(duration):
    with pytest.raises(ValueError, match="nonnegative and finite"):
        LLMTokenAutoscalingPolicy(10, duration)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
