import asyncio
import sys
import time
from typing import Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

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


def _make_autoscaling_context(prometheus_queries=None) -> AutoscalingContext:
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
        prometheus_queries=prometheus_queries,
    )


class _AsyncCM:
    """Wrap a coroutine as an async context manager."""

    def __init__(self, coro):
        self._coro = coro

    async def __aenter__(self):
        return await self._coro

    async def __aexit__(self, *args):
        pass


class _SessionCM:
    """Wrap a mock session as an async context manager."""

    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, *args):
        pass


def _make_mock_session(responses: Dict[str, dict]):
    """Build a mock aiohttp session returning canned Prometheus responses.

    Args:
        responses: mapping of PromQL query string -> JSON response body.

    Returns:
        A mock session whose ``get`` method returns matching responses.
    """

    async def _mock_get(url, params=None, timeout=None):
        query = params["query"]
        body = responses.get(query, {"data": {"result": []}})
        resp = AsyncMock()
        resp.raise_for_status = MagicMock()
        resp.json = AsyncMock(return_value=body)
        return resp

    session = AsyncMock()
    session.get = MagicMock(side_effect=lambda *a, **kw: _AsyncCM(_mock_get(*a, **kw)))
    return session


def _make_state(
    prometheus_queries: Optional[list] = None,
) -> DeploymentAutoscalingState:
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


class TestAutoscalingContextPrometheusQueries:
    def test_none_when_not_configured(self):
        ctx = _make_autoscaling_context(prometheus_queries=None)
        assert ctx.prometheus_queries is None

    def test_dict_value(self):
        ctx = _make_autoscaling_context(prometheus_queries={"rate(http_rps[5m])": 42.0})
        assert ctx.prometheus_queries == {"rate(http_rps[5m])": 42.0}

    def test_lazy_callable(self):
        call_count = 0

        def fetch():
            nonlocal call_count
            call_count += 1
            return {"lazy": 99.0}

        ctx = _make_autoscaling_context(prometheus_queries=fetch)
        assert call_count == 0
        assert ctx.prometheus_queries == {"lazy": 99.0}
        assert call_count == 1
        # Cached
        _ = ctx.prometheus_queries
        assert call_count == 1


# ---------------------------------------------------------------------------
# DeploymentAutoscalingState tests
# ---------------------------------------------------------------------------


class TestDeploymentAutoscalingStatePrometheus:
    def test_cache_returns_none_before_fetch(self):
        state = _make_state(prometheus_queries=["up"])
        assert state._get_cached_prometheus_metrics() is None

    def test_has_prometheus_queries(self):
        assert _make_state(prometheus_queries=["up"]).has_prometheus_queries()
        assert not _make_state(prometheus_queries=None).has_prometheus_queries()
        assert not _make_state(prometheus_queries=[]).has_prometheus_queries()

    @pytest.mark.asyncio
    async def test_fetch_caches_results(self):
        state = _make_state(prometheus_queries=["my_gauge", "rate(rps[5m])"])
        responses = {
            "my_gauge": {"data": {"result": [{"value": [0, "42.5"]}]}},
            "rate(rps[5m])": {"data": {"result": [{"value": [0, "100"]}]}},
        }
        session = _make_mock_session(responses)

        with patch(
            "ray.serve._private.autoscaling_state.RAY_SERVE_PROMETHEUS_SERVER_ADDRESS",
            "localhost:9090",
        ):
            await state.fetch_prometheus_metrics(session)

        assert state._get_cached_prometheus_metrics() == {
            "my_gauge": 42.5,
            "rate(rps[5m])": 100.0,
        }

    @pytest.mark.asyncio
    async def test_fetch_batches_with_gather(self):
        """All queries in a deployment should run concurrently."""
        state = _make_state(prometheus_queries=["a", "b", "c"])
        call_order = []

        async def _mock_get(url, params=None, timeout=None):
            query = params["query"]
            call_order.append(query)
            resp = AsyncMock()
            resp.raise_for_status = MagicMock()
            resp.json = AsyncMock(
                return_value={"data": {"result": [{"value": [0, "1"]}]}}
            )
            return resp

        session = AsyncMock()
        session.get = MagicMock(
            side_effect=lambda *a, **kw: _AsyncCM(_mock_get(*a, **kw))
        )

        with patch(
            "ray.serve._private.autoscaling_state.RAY_SERVE_PROMETHEUS_SERVER_ADDRESS",
            "prom:9090",
        ):
            await state.fetch_prometheus_metrics(session)

        # All three queries were issued
        assert sorted(call_order) == ["a", "b", "c"]
        assert state._get_cached_prometheus_metrics() == {
            "a": 1.0,
            "b": 1.0,
            "c": 1.0,
        }

    @pytest.mark.asyncio
    async def test_empty_result_caches_none(self):
        state = _make_state(prometheus_queries=["missing"])
        session = _make_mock_session({"missing": {"data": {"result": []}}})

        with patch(
            "ray.serve._private.autoscaling_state.RAY_SERVE_PROMETHEUS_SERVER_ADDRESS",
            "localhost:9090",
        ):
            await state.fetch_prometheus_metrics(session)

        assert state._get_cached_prometheus_metrics() is None

    @pytest.mark.asyncio
    async def test_partial_failure(self):
        """One failing query should not prevent others from caching."""
        state = _make_state(prometheus_queries=["good", "bad"])

        async def _mock_get(url, params=None, timeout=None):
            query = params["query"]
            if query == "bad":
                raise Exception("connection refused")
            resp = AsyncMock()
            resp.raise_for_status = MagicMock()
            resp.json = AsyncMock(
                return_value={"data": {"result": [{"value": [0, "7"]}]}}
            )
            return resp

        session = AsyncMock()
        session.get = MagicMock(
            side_effect=lambda *a, **kw: _AsyncCM(_mock_get(*a, **kw))
        )

        with patch(
            "ray.serve._private.autoscaling_state.RAY_SERVE_PROMETHEUS_SERVER_ADDRESS",
            "localhost:9090",
        ):
            await state.fetch_prometheus_metrics(session)

        assert state._get_cached_prometheus_metrics() == {"good": 7.0}

    def test_cache_expiry(self):
        state = _make_state(prometheus_queries=["m"])
        state._cached_prometheus_metrics = {"m": 1.0}

        # Fresh cache
        state._prometheus_cache_timestamp = time.time()
        assert state._get_cached_prometheus_metrics() == {"m": 1.0}

        # Expired cache
        state._prometheus_cache_timestamp = time.time() - 9999
        assert state._get_cached_prometheus_metrics() is None


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


# ---------------------------------------------------------------------------
# Feature flag / short-circuit tests
# ---------------------------------------------------------------------------


class TestFeatureFlagAndShortCircuit:
    def test_manager_short_circuits_when_disabled(self):
        mgr = AutoscalingStateManager()
        # Even if there are app states, the flag being off should short-circuit
        with patch(
            "ray.serve._private.autoscaling_state.RAY_SERVE_ENABLE_PROMETHEUS_AUTOSCALING",
            False,
        ):
            assert not mgr._any_prometheus_queries()

    def test_manager_short_circuits_when_no_queries(self):
        mgr = AutoscalingStateManager()
        with patch(
            "ray.serve._private.autoscaling_state.RAY_SERVE_ENABLE_PROMETHEUS_AUTOSCALING",
            True,
        ):
            assert not mgr._any_prometheus_queries()

    def test_start_noop_when_no_queries(self):
        mgr = AutoscalingStateManager()
        loop = asyncio.new_event_loop()
        try:
            mgr.start_prometheus_fetch_loop(loop)
            assert not hasattr(mgr, "_prometheus_task") or mgr._prometheus_task is None
        finally:
            loop.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
