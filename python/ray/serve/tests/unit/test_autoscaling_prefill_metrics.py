import math
import sys
import time
from unittest.mock import Mock, patch

import pytest

from ray.serve._private.autoscaling_metrics_codec import decode_handle_flat, encode
from ray.serve._private.autoscaling_state import (
    ApplicationAutoscalingState,
    DeploymentAutoscalingState,
)
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    HandleMetricReport,
    TimeStampedValue,
)
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve.config import AutoscalingConfig

PREFILL = "outstanding_effective_prefill_tokens"


@pytest.fixture
def state():
    state = DeploymentAutoscalingState(DeploymentID("D", "app"))
    state.register(
        DeploymentInfo(
            deployment_config=DeploymentConfig(
                autoscaling_config=AutoscalingConfig(min_replicas=1, max_replicas=10)
            ),
            replica_config=ReplicaConfig.create(lambda: None),
            start_time_ms=0,
            deployer_job_id="",
        ),
        curr_target_num_replicas=1,
    )
    state.autoscaling_outstanding_prefill_tokens_gauge = Mock()
    return state


def report(state, handle_id, values, *, actor_id=None):
    now = time.time()
    return HandleMetricReport(
        deployment_id=state._deployment_id,
        handle_id=handle_id,
        actor_id=actor_id,
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=[],
        metrics={},
        timestamp=now,
        router_metrics={
            PREFILL: [
                TimeStampedValue(now - len(values) + i, value)
                for i, value in enumerate(values)
            ]
        },
    )


@pytest.mark.parametrize("columnar", [False, True])
@pytest.mark.parametrize(
    "histories, expected",
    [
        ([[12000]], 12000),
        ([[12000], [12000]], 12000),
        ([[30000, 1000], [2000, 9000]], 9000),
        ([[], [4000]], 4000),
        ([[0]], 0),
        ([[-100]], 0),
    ],
)
def test_prefill_metric_from_router_reports(state, columnar, histories, expected):
    for i, values in enumerate(histories):
        metric_report = report(state, str(i), values)
        if columnar:
            state.record_columnar_metrics_for_handle(
                decode_handle_flat(encode(metric_report))
            )
        else:
            state.record_request_metrics_for_handle(metric_report)

    # The default policy does not return token metrics in its state.
    state.get_decision_num_replicas(1)
    state.autoscaling_outstanding_prefill_tokens_gauge.set.assert_called_once_with(
        expected, tags={"deployment": "D", "application": "app"}
    )


@pytest.mark.parametrize("columnar", [False, True])
@pytest.mark.parametrize("removal", ["dead_actor", "timeout"])
def test_prefill_metric_unavailable_and_recovers(state, columnar, removal):
    gauge = state.autoscaling_outstanding_prefill_tokens_gauge
    state.get_decision_num_replicas(1)
    state.record_request_metrics_for_handle(report(state, "empty", []))
    state.get_decision_num_replicas(1)
    gauge.set.assert_not_called()

    metric_report = report(state, "router", [12345], actor_id="proxy")
    if columnar:
        state.record_columnar_metrics_for_handle(
            decode_handle_flat(encode(metric_report))
        )
    else:
        state.record_request_metrics_for_handle(metric_report)
    state.get_decision_num_replicas(1)
    assert gauge.set.call_args.args == (12345,)

    if removal == "dead_actor":
        state.drop_stale_handle_metrics(set())
    else:
        with patch("time.monotonic", return_value=time.monotonic() + 10000):
            state.drop_stale_handle_metrics({"proxy"})
    state.get_decision_num_replicas(1)
    assert math.isnan(gauge.set.call_args.args[0])

    state.record_request_metrics_for_handle(report(state, "router", [0]))
    state.get_decision_num_replicas(1)
    assert gauge.set.call_args.args == (0,)


def test_prefill_metric_disappears_from_live_report(state):
    state.record_request_metrics_for_handle(report(state, "router", [100]))
    state.get_decision_num_replicas(1)
    state.record_request_metrics_for_handle(report(state, "router", []))
    state.get_decision_num_replicas(1)
    assert math.isnan(
        state.autoscaling_outstanding_prefill_tokens_gauge.set.call_args.args[0]
    )


def test_prefill_metric_application_policy(state):
    state.record_request_metrics_for_handle(report(state, "router", [5000]))
    app = ApplicationAutoscalingState("app")
    dep_id = state._deployment_id
    app._deployment_autoscaling_states[dep_id] = state
    app._policy = lambda contexts: ({dep_id: 1}, {})
    app.get_decision_num_replicas({dep_id: 1})
    state.autoscaling_outstanding_prefill_tokens_gauge.set.assert_called_once_with(
        5000, tags={"deployment": "D", "application": "app"}
    )


def test_prefill_metric_cold_start_bypasses_policy(state):
    metric_report = report(state, "router", [5000])
    metric_report.queued_requests = [TimeStampedValue(time.time() - 1, 1)]
    state.record_request_metrics_for_handle(metric_report)
    state.get_decision_num_replicas(0)
    state.autoscaling_outstanding_prefill_tokens_gauge.set.assert_called_once_with(
        5000, tags={"deployment": "D", "application": "app"}
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
