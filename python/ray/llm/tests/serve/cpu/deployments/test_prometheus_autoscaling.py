import sys
import time

import pytest

from ray.serve._private.common import DeploymentID
from ray.serve.config import AutoscalingContext
from ray.serve.llm.autoscaling import P99_TTFT_QUERY, TTFTAutoscalingPolicy


def _ctx(current=2, total_requests=0.0):
    return AutoscalingContext(
        deployment_id=DeploymentID(name="d", app_name="a"),
        deployment_name="d",
        app_name="a",
        current_num_replicas=current,
        target_num_replicas=current,
        running_replicas=[],
        total_num_requests=total_requests,
        total_queued_requests=0.0,
        aggregated_metrics={},
        raw_metrics={},
        capacity_adjusted_min_replicas=0,
        capacity_adjusted_max_replicas=10,
        policy_state={},
        last_scale_up_time=None,
        last_scale_down_time=None,
        current_time=None,
        config=None,
        total_pending_async_requests=0,
    )


def _policy_with_cache(values, ttft_target_s=1.0, idle_threshold=1.0):
    """A policy with its cache pre-populated and the refresh thread disabled."""
    pol = TTFTAutoscalingPolicy(
        ttft_target_s=ttft_target_s,
        idle_threshold=idle_threshold,
        prometheus_address="localhost:9090",
    )
    pol._started = True  # skip the background fetch thread
    pol._cache.values = values
    pol._cache.timestamp = time.monotonic()
    return pol


class TestTTFTPolicyDecisions:
    def test_no_metrics_holds(self):
        pol = TTFTAutoscalingPolicy(prometheus_address=None)
        pol._started = True
        assert pol(_ctx(current=2)) == (2.0, {"signal": "no_metrics"})

    def test_missing_query_is_no_data(self):
        dec, state = _policy_with_cache({"other": 1.0})(_ctx(current=2))
        assert dec == 2.0 and state["signal"] == "no_data"

    def test_scale_up_above_target(self):
        dec, state = _policy_with_cache({P99_TTFT_QUERY: 5.0})(_ctx(current=2))
        assert dec == 3.0 and state["signal"] == "scale_up"

    def test_scale_down_when_idle(self):
        dec, state = _policy_with_cache({P99_TTFT_QUERY: 0.1})(
            _ctx(current=3, total_requests=0.0)
        )
        assert dec == 2.0 and state["signal"] == "scale_down"

    def test_steady_when_busy(self):
        dec, state = _policy_with_cache({P99_TTFT_QUERY: 0.1})(
            _ctx(current=3, total_requests=9.0)
        )
        assert dec == 3.0 and state["signal"] == "steady"

    def test_scale_down_floors_at_zero(self):
        dec, state = _policy_with_cache({P99_TTFT_QUERY: 0.1})(
            _ctx(current=1, total_requests=0.0)
        )
        assert dec == 0.0 and state["signal"] == "scale_down"


class TestQueryScoping:
    def test_model_id_scopes_query(self):
        pol = TTFTAutoscalingPolicy(model_id="my-org/m", prometheus_address="x")
        assert 'model_name="my-org/m"' in pol.query

    def test_default_query_is_unscoped(self):
        pol = TTFTAutoscalingPolicy(prometheus_address="x")
        assert "model_name" not in pol.query
        assert pol.query == P99_TTFT_QUERY

    def test_explicit_query_overrides_model_id(self):
        pol = TTFTAutoscalingPolicy(
            model_id="my-org/m", query="custom_query", prometheus_address="x"
        )
        assert pol.query == "custom_query"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
