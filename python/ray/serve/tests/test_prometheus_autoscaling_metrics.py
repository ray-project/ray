import sys
from typing import Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ray.serve._private.autoscaling_state import DeploymentAutoscalingState
from ray.serve._private.common import DeploymentID
from ray.serve.config import AutoscalingConfig, AutoscalingContext


def _make_autoscaling_context(
    prometheus_metrics=None,
) -> AutoscalingContext:
    """Helper to build an AutoscalingContext with minimal required fields."""
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


def _make_mock_aiohttp_session(responses: Dict[str, dict]):
    """Create a mock aiohttp.ClientSession that returns canned responses.

    Args:
        responses: mapping of metric query -> JSON response body.

    Returns:
        A mock session object.
    """

    async def mock_get(url, params=None, timeout=None):
        query = params["query"]
        body = responses.get(query, {"data": {"result": []}})
        resp = AsyncMock()
        resp.raise_for_status = MagicMock()
        resp.json = AsyncMock(return_value=body)
        return resp

    session = AsyncMock()
    session.get = MagicMock(side_effect=lambda *a, **kw: _async_cm(mock_get(*a, **kw)))
    return session


class _async_cm:
    """Wrap a coroutine as an async context manager (for `async with`)."""

    def __init__(self, coro):
        self._coro = coro

    async def __aenter__(self):
        return await self._coro

    async def __aexit__(self, *args):
        pass


class _session_cm:
    """Wrap a session mock as an async context manager."""

    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, *args):
        pass


class TestAutoscalingContextPrometheusMetrics:
    """Test that AutoscalingContext exposes prometheus_metrics correctly."""

    def test_none_when_not_configured(self):
        ctx = _make_autoscaling_context(prometheus_metrics=None)
        assert ctx.prometheus_metrics is None

    def test_dict_value(self):
        ctx = _make_autoscaling_context(
            prometheus_metrics={"my_metric": 42.0, "other": 1.5}
        )
        assert ctx.prometheus_metrics == {"my_metric": 42.0, "other": 1.5}

    def test_lazy_callable(self):
        """prometheus_metrics supports lazy evaluation via a callable."""
        call_count = 0

        def fetch():
            nonlocal call_count
            call_count += 1
            return {"lazy_metric": 99.0}

        ctx = _make_autoscaling_context(prometheus_metrics=fetch)
        assert call_count == 0
        assert ctx.prometheus_metrics == {"lazy_metric": 99.0}
        assert call_count == 1
        # Cached on second access
        _ = ctx.prometheus_metrics
        assert call_count == 1


class TestDeploymentAutoscalingStatePrometheus:
    """Test the controller-side async Prometheus fetch + cache."""

    def _make_state_with_config(
        self, prometheus_metrics: Optional[list] = None
    ) -> DeploymentAutoscalingState:
        state = DeploymentAutoscalingState(
            DeploymentID(name="MyDeployment", app_name="app")
        )
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=5,
            prometheus_metrics=prometheus_metrics,
        )
        state._config = config
        return state

    def test_cache_returns_none_when_not_configured(self):
        state = self._make_state_with_config(prometheus_metrics=None)
        assert state._get_cached_prometheus_metrics() is None

    def test_cache_returns_none_before_first_fetch(self):
        state = self._make_state_with_config(prometheus_metrics=["m"])
        assert state._get_cached_prometheus_metrics() is None

    @pytest.mark.asyncio
    async def test_fetch_populates_cache(self):
        state = self._make_state_with_config(
            prometheus_metrics=["my_gauge", "my_counter"]
        )

        responses = {
            "my_gauge": {"data": {"result": [{"value": [0, "42.5"]}]}},
            "my_counter": {"data": {"result": [{"value": [0, "100"]}]}},
        }
        session = _make_mock_aiohttp_session(responses)

        with patch(
            "ray.serve._private.autoscaling_state.RAY_SERVE_PROMETHEUS_SERVER_ADDRESS",
            "localhost:9090",
        ):
            with patch(
                "ray.serve._private.autoscaling_state.aiohttp.ClientSession",
                return_value=_session_cm(session),
            ):
                await state.fetch_prometheus_metrics()

        assert state._get_cached_prometheus_metrics() == {
            "my_gauge": 42.5,
            "my_counter": 100.0,
        }

    @pytest.mark.asyncio
    async def test_fetch_with_no_server_address_caches_none(self):
        state = self._make_state_with_config(prometheus_metrics=["m"])

        with patch(
            "ray.serve._private.autoscaling_state.RAY_SERVE_PROMETHEUS_SERVER_ADDRESS",
            None,
        ):
            await state.fetch_prometheus_metrics()

        assert state._get_cached_prometheus_metrics() is None

    @pytest.mark.asyncio
    async def test_fetch_with_empty_result_caches_none(self):
        state = self._make_state_with_config(prometheus_metrics=["missing"])

        responses = {"missing": {"data": {"result": []}}}
        session = _make_mock_aiohttp_session(responses)

        with patch(
            "ray.serve._private.autoscaling_state.RAY_SERVE_PROMETHEUS_SERVER_ADDRESS",
            "localhost:9090",
        ):
            with patch(
                "ray.serve._private.autoscaling_state.aiohttp.ClientSession",
                return_value=_session_cm(session),
            ):
                await state.fetch_prometheus_metrics()

        assert state._get_cached_prometheus_metrics() is None

    @pytest.mark.asyncio
    async def test_fetch_handles_connection_error(self):
        state = self._make_state_with_config(prometheus_metrics=["m"])

        async def raise_error(*a, **kw):
            raise Exception("connection refused")

        session = AsyncMock()
        session.get = MagicMock(side_effect=lambda *a, **kw: _async_cm(raise_error()))

        with patch(
            "ray.serve._private.autoscaling_state.RAY_SERVE_PROMETHEUS_SERVER_ADDRESS",
            "localhost:9090",
        ):
            with patch(
                "ray.serve._private.autoscaling_state.aiohttp.ClientSession",
                return_value=_session_cm(session),
            ):
                await state.fetch_prometheus_metrics()

        assert state._get_cached_prometheus_metrics() is None

    @pytest.mark.asyncio
    async def test_partial_failure_caches_successful_metrics(self):
        state = self._make_state_with_config(
            prometheus_metrics=["good_metric", "bad_metric"]
        )

        call_count = 0

        async def mock_get(url, params=None, timeout=None):
            nonlocal call_count
            call_count += 1
            query = params["query"]
            if query == "bad_metric":
                raise Exception("connection refused")
            resp = AsyncMock()
            resp.raise_for_status = MagicMock()
            resp.json = AsyncMock(
                return_value={"data": {"result": [{"value": [0, "7.0"]}]}}
            )
            return resp

        session = AsyncMock()
        session.get = MagicMock(
            side_effect=lambda *a, **kw: _async_cm(mock_get(*a, **kw))
        )

        with patch(
            "ray.serve._private.autoscaling_state.RAY_SERVE_PROMETHEUS_SERVER_ADDRESS",
            "localhost:9090",
        ):
            with patch(
                "ray.serve._private.autoscaling_state.aiohttp.ClientSession",
                return_value=_session_cm(session),
            ):
                await state.fetch_prometheus_metrics()

        assert state._get_cached_prometheus_metrics() == {"good_metric": 7.0}
        assert call_count == 2


class TestAutoscalingConfigPrometheusField:
    """Test the prometheus_metrics field on AutoscalingConfig."""

    def test_default_is_none(self):
        config = AutoscalingConfig(min_replicas=1, max_replicas=5)
        assert config.prometheus_metrics is None

    def test_accepts_list_of_strings(self):
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=5,
            prometheus_metrics=["metric_a", "metric_b"],
        )
        assert config.prometheus_metrics == ["metric_a", "metric_b"]

    def test_serialization_roundtrip(self):
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=5,
            prometheus_metrics=["m1"],
        )
        data = config.model_dump()
        restored = AutoscalingConfig(**data)
        assert restored.prometheus_metrics == ["m1"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
