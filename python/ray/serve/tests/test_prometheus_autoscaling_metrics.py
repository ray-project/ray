import sys
import time

import pytest

from ray.serve._private.autoscaling_state import (
    AutoscalingStateManager,
    DeploymentAutoscalingState,
)
from ray.serve._private.common import DeploymentID
from ray.serve.config import AutoscalingConfig, AutoscalingContext

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_autoscaling_context(prometheus_metrics=None) -> AutoscalingContext:
    """Build a minimal AutoscalingContext."""
    return AutoscalingContext(
        deployment_id=DeploymentID(name="test", app_name="app"),
        deployment_name="test",
        app_name="app",
        current_num_replicas=1,
        target_num_replicas=1,
        running_replicas=[],
        total_num_requests=0.0,
        total_queued_requests=0.0,
        aggregated_metrics=None,
        raw_metrics=None,
        capacity_adjusted_min_replicas=1,
        capacity_adjusted_max_replicas=10,
        policy_state={},
        last_scale_up_time=None,
        last_scale_down_time=None,
        current_time=None,
        config=None,
        total_pending_async_requests=0,
        prometheus_metrics=prometheus_metrics,
    )


def _make_state(prometheus_queries=None) -> DeploymentAutoscalingState:
    """Build a DeploymentAutoscalingState with config set directly."""
    state = DeploymentAutoscalingState(
        DeploymentID(name="MyDeployment", app_name="app")
    )
    state._config = AutoscalingConfig(
        min_replicas=1,
        max_replicas=5,
        prometheus_queries=prometheus_queries,
    )
    return state


# ---------------------------------------------------------------------------
# AutoscalingContext tests
# ---------------------------------------------------------------------------


class TestAutoscalingContextPrometheusMetrics:
    def test_none_when_not_configured(self):
        ctx = _make_autoscaling_context(prometheus_metrics=None)
        assert ctx.prometheus_metrics is None

    def test_dict_value(self):
        ctx = _make_autoscaling_context(prometheus_metrics={"rate(http_rps[5m])": 42.0})
        assert ctx.prometheus_metrics == {"rate(http_rps[5m])": 42.0}

    def test_lazy_callable(self):
        call_count = 0

        def fetch():
            nonlocal call_count
            call_count += 1
            return {"lazy": 99.0}

        ctx = _make_autoscaling_context(prometheus_metrics=fetch)
        assert call_count == 0
        assert ctx.prometheus_metrics == {"lazy": 99.0}
        assert call_count == 1
        # Cached
        _ = ctx.prometheus_metrics
        assert call_count == 1


# ---------------------------------------------------------------------------
# DeploymentAutoscalingState cache tests
# ---------------------------------------------------------------------------


class TestDeploymentAutoscalingStatePrometheus:
    def test_cache_returns_none_before_record(self):
        state = _make_state(prometheus_queries=["up"])
        assert state._get_cached_prometheus_metrics() is None

    def test_has_prometheus_queries(self):
        assert _make_state(prometheus_queries=["up"]).has_prometheus_queries()
        assert not _make_state(prometheus_queries=None).has_prometheus_queries()
        assert not _make_state(prometheus_queries=[]).has_prometheus_queries()

    def test_record_populates_cache(self):
        state = _make_state(prometheus_queries=["my_gauge", "rate(rps[5m])"])
        state.record_prometheus_metrics(
            {"my_gauge": 42.5, "rate(rps[5m])": 100.0},
            timestamp=time.time(),
        )
        assert state._get_cached_prometheus_metrics() == {
            "my_gauge": 42.5,
            "rate(rps[5m])": 100.0,
        }

    def test_record_empty_caches_none(self):
        state = _make_state(prometheus_queries=["m"])
        state.record_prometheus_metrics({}, timestamp=time.time())
        assert state._get_cached_prometheus_metrics() is None

    def test_cache_expiry(self):
        state = _make_state(prometheus_queries=["m"])
        state.record_prometheus_metrics({"m": 1.0}, timestamp=time.time())
        assert state._get_cached_prometheus_metrics() == {"m": 1.0}

        # Expired cache
        state._prometheus_cache_timestamp = time.time() - 9999
        assert state._get_cached_prometheus_metrics() is None


# ---------------------------------------------------------------------------
# AutoscalingStateManager record + query extraction tests
# ---------------------------------------------------------------------------


class TestAutoscalingStateManagerPrometheus:
    def test_record_routes_to_deployment(self):
        mgr = AutoscalingStateManager()
        dep_id = DeploymentID(name="D", app_name="app")

        # We need a registered deployment for record to work.
        # Manually set up the internal state.
        from ray.serve._private.autoscaling_state import (
            ApplicationAutoscalingState,
            DeploymentAutoscalingState,
        )

        app_state = ApplicationAutoscalingState("app")
        dep_state = DeploymentAutoscalingState(dep_id)
        dep_state._config = AutoscalingConfig(
            min_replicas=1, max_replicas=5, prometheus_queries=["q"]
        )
        app_state._deployment_autoscaling_states[dep_id] = dep_state
        mgr._app_autoscaling_states["app"] = app_state

        mgr.record_prometheus_metrics({dep_id: {"q": 7.0}}, time.time())
        assert dep_state._get_cached_prometheus_metrics() == {"q": 7.0}

    def test_get_queries_by_deployment(self):
        mgr = AutoscalingStateManager()
        dep_id = DeploymentID(name="D", app_name="app")

        from ray.serve._private.autoscaling_state import (
            ApplicationAutoscalingState,
            DeploymentAutoscalingState,
        )

        app_state = ApplicationAutoscalingState("app")
        dep_state = DeploymentAutoscalingState(dep_id)
        dep_state._config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=5,
            prometheus_queries=["rate(rps[5m])", "up"],
        )
        app_state._deployment_autoscaling_states[dep_id] = dep_state
        mgr._app_autoscaling_states["app"] = app_state

        queries = mgr.get_prometheus_queries_by_deployment()
        assert queries == {dep_id: ["rate(rps[5m])", "up"]}

    def test_get_queries_empty_when_none_configured(self):
        mgr = AutoscalingStateManager()
        assert mgr.get_prometheus_queries_by_deployment() == {}


# ---------------------------------------------------------------------------
# AutoscalingConfig field tests
# ---------------------------------------------------------------------------


class TestAutoscalingConfigPrometheusField:
    def test_default_is_none(self):
        config = AutoscalingConfig(min_replicas=1, max_replicas=5)
        assert config.prometheus_queries is None

    def test_accepts_promql_expressions(self):
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=5,
            prometheus_queries=[
                "rate(http_requests_total[5m])",
                "histogram_quantile(0.99, sum(rate(latency_bucket[5m])) by (le))",
            ],
        )
        assert len(config.prometheus_queries) == 2

    def test_serialization_roundtrip(self):
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=5,
            prometheus_queries=["rate(rps[5m])"],
        )
        data = config.model_dump()
        restored = AutoscalingConfig(**data)
        assert restored.prometheus_queries == ["rate(rps[5m])"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
