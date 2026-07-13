import asyncio
import sys
import time
import types

import pytest

import ray.serve._private.controller as controller_mod
from ray.serve._private.autoscaling_state import (
    ApplicationAutoscalingState,
    AutoscalingStateManager,
    DeploymentAutoscalingState,
)
from ray.serve._private.common import DeploymentID
from ray.serve._private.controller import ServeController
from ray.serve._private.prometheus_metrics import fetch_metrics, normalize_query_url
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


def _make_state(
    prometheus_queries=None, prometheus_address=None
) -> DeploymentAutoscalingState:
    """Build a DeploymentAutoscalingState with config set directly."""
    state = DeploymentAutoscalingState(
        DeploymentID(name="MyDeployment", app_name="app")
    )
    state._config = AutoscalingConfig(
        min_replicas=1,
        max_replicas=5,
        prometheus_queries=prometheus_queries,
        prometheus_address=prometheus_address,
    )
    return state


def _register_deployment(
    mgr, dep_id, prometheus_queries, prometheus_address
) -> DeploymentAutoscalingState:
    """Register a deployment with Prometheus config on the manager's state."""
    app_state = ApplicationAutoscalingState(dep_id.app_name)
    dep_state = _make_state(prometheus_queries, prometheus_address)
    dep_state._deployment_id = dep_id
    app_state._deployment_autoscaling_states[dep_id] = dep_state
    mgr._app_autoscaling_states[dep_id.app_name] = app_state
    return dep_state


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
        state = _make_state(
            prometheus_queries=["up"], prometheus_address="localhost:9090"
        )
        assert state._get_cached_prometheus_metrics() is None

    def test_has_prometheus_queries(self):
        assert _make_state(
            prometheus_queries=["up"], prometheus_address="localhost:9090"
        ).has_prometheus_queries()
        assert not _make_state(prometheus_queries=None).has_prometheus_queries()
        assert not _make_state(prometheus_queries=[]).has_prometheus_queries()
        # queries without address should also be False
        assert not _make_state(prometheus_queries=["up"]).has_prometheus_queries()

    def test_record_populates_cache(self):
        state = _make_state(
            prometheus_queries=["my_gauge", "rate(rps[5m])"],
            prometheus_address="localhost:9090",
        )
        state.record_prometheus_metrics(
            {"my_gauge": 42.5, "rate(rps[5m])": 100.0},
            timestamp=time.time(),
        )
        assert state._get_cached_prometheus_metrics() == {
            "my_gauge": 42.5,
            "rate(rps[5m])": 100.0,
        }

    def test_record_empty_caches_none(self):
        state = _make_state(
            prometheus_queries=["m"], prometheus_address="localhost:9090"
        )
        state.record_prometheus_metrics({}, timestamp=time.time())
        assert state._get_cached_prometheus_metrics() is None

    def test_cache_expiry(self):
        state = _make_state(
            prometheus_queries=["m"], prometheus_address="localhost:9090"
        )
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
        dep_state = _register_deployment(mgr, dep_id, ["q"], "localhost:9090")
        mgr.record_prometheus_metrics({dep_id: {"q": 7.0}}, time.time())
        assert dep_state._get_cached_prometheus_metrics() == {"q": 7.0}

    def test_get_config_by_deployment(self):
        mgr = AutoscalingStateManager()
        dep_id = DeploymentID(name="D", app_name="app")
        _register_deployment(mgr, dep_id, ["rate(rps[5m])", "up"], "localhost:9090")
        assert mgr.get_prometheus_config_by_deployment() == {
            dep_id: (["rate(rps[5m])", "up"], "localhost:9090")
        }

    def test_get_config_empty_when_none_configured(self):
        assert AutoscalingStateManager().get_prometheus_config_by_deployment() == {}

    def test_get_config_empty_when_no_address(self):
        # Queries without an address are not returned.
        mgr = AutoscalingStateManager()
        dep_id = DeploymentID(name="D", app_name="app")
        _register_deployment(mgr, dep_id, ["up"], None)
        assert mgr.get_prometheus_config_by_deployment() == {}


# ---------------------------------------------------------------------------
# AutoscalingConfig field tests
# ---------------------------------------------------------------------------


class TestAutoscalingConfigPrometheusField:
    def test_default_is_none(self):
        config = AutoscalingConfig(min_replicas=1, max_replicas=5)
        assert config.prometheus_queries is None
        assert config.prometheus_address is None

    def test_accepts_promql_expressions(self):
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=5,
            prometheus_address="localhost:9090",
            prometheus_queries=[
                "rate(http_requests_total[5m])",
                "histogram_quantile(0.99, sum(rate(latency_bucket[5m])) by (le))",
            ],
        )
        assert len(config.prometheus_queries) == 2
        assert config.prometheus_address == "localhost:9090"

    def test_serialization_roundtrip(self):
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=5,
            prometheus_address="localhost:9090",
            prometheus_queries=["rate(rps[5m])"],
        )
        data = config.model_dump()
        restored = AutoscalingConfig(**data)
        assert restored.prometheus_queries == ["rate(rps[5m])"]
        assert restored.prometheus_address == "localhost:9090"


# ---------------------------------------------------------------------------
# Prometheus fetch helper tests
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Stand-in for an aiohttp response used as an async context manager."""

    def __init__(self, payload=None, exc=None):
        self._payload = payload
        self._exc = exc

    async def __aenter__(self):
        if self._exc is not None:
            raise self._exc
        return self

    async def __aexit__(self, *exc_info):
        return False

    def raise_for_status(self):
        pass

    async def json(self):
        return self._payload


class _FakeSession:
    """Maps each PromQL query to a payload dict or an exception to raise."""

    def __init__(self, responses):
        self._responses = responses

    def get(self, url, params=None, timeout=None):
        outcome = self._responses[params["query"]]
        if isinstance(outcome, Exception):
            return _FakeResponse(exc=outcome)
        return _FakeResponse(payload=outcome)


def _vector_payload(value: float) -> dict:
    return {
        "status": "success",
        "data": {"resultType": "vector", "result": [{"value": [0, str(value)]}]},
    }


class TestNormalizeQueryUrl:
    @pytest.mark.parametrize(
        "address,expected",
        [
            ("localhost:9090", "http://localhost:9090/api/v1/query"),
            ("http://localhost:9090", "http://localhost:9090/api/v1/query"),
            ("https://prom:9090", "https://prom:9090/api/v1/query"),
            ("localhost:9090/", "http://localhost:9090/api/v1/query"),
            # Already a full query endpoint: left as-is.
            ("http://prom:9090/api/v1/query", "http://prom:9090/api/v1/query"),
        ],
    )
    def test_normalize(self, address, expected):
        assert normalize_query_url(address) == expected


class TestFetchMetrics:
    def test_parses_scalar_from_vector(self):
        session = _FakeSession({"q1": _vector_payload(2.5), "q2": _vector_payload(7.0)})
        result = asyncio.run(fetch_metrics(session, "localhost:9090", ["q1", "q2"]))
        assert result == {"q1": 2.5, "q2": 7.0}

    def test_omits_failed_and_empty_queries(self):
        session = _FakeSession(
            {
                "ok": _vector_payload(1.0),
                "empty": {
                    "status": "success",
                    "data": {"resultType": "vector", "result": []},
                },
                "boom": RuntimeError("connection refused"),
            }
        )
        result = asyncio.run(
            fetch_metrics(session, "localhost:9090", ["ok", "empty", "boom"])
        )
        assert result == {"ok": 1.0}

    def test_omits_nan_and_inf(self):
        session = _FakeSession(
            {
                "finite": _vector_payload(3.0),
                "nan": _vector_payload(float("nan")),
                "inf": _vector_payload(float("inf")),
            }
        )
        result = asyncio.run(
            fetch_metrics(session, "localhost:9090", ["finite", "nan", "inf"])
        )
        assert result == {"finite": 3.0}

    def test_accepts_scalar_result_type(self):
        session = _FakeSession(
            {
                "s": {
                    "status": "success",
                    "data": {"resultType": "scalar", "result": [0, "3.5"]},
                }
            }
        )
        result = asyncio.run(fetch_metrics(session, "localhost:9090", ["s"]))
        assert result == {"s": 3.5}

    def test_rejects_multi_sample_vector(self):
        # A query that matches multiple series is not a scalar signal.
        session = _FakeSession(
            {
                "multi": {
                    "status": "success",
                    "data": {
                        "resultType": "vector",
                        "result": [{"value": [0, "1.0"]}, {"value": [0, "2.0"]}],
                    },
                }
            }
        )
        result = asyncio.run(fetch_metrics(session, "localhost:9090", ["multi"]))
        assert result == {}


# ---------------------------------------------------------------------------
# Controller background-fetch orchestration
# ---------------------------------------------------------------------------


class _RecordingStateManager:
    """Stand-in AutoscalingStateManager for the controller fetch methods."""

    def __init__(self, prom_config):
        self._prom_config = prom_config
        self.recorded = None

    def get_prometheus_config_by_deployment(self):
        return self._prom_config

    def record_prometheus_metrics(self, metrics_by_deployment, timestamp):
        self.recorded = (metrics_by_deployment, timestamp)


class _NoopSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


def _controller_stub(prom_config, last_fetch_time=0.0):
    return types.SimpleNamespace(
        _prometheus_fetch_task=None,
        _last_prometheus_fetch_time=last_fetch_time,
        autoscaling_state_manager=_RecordingStateManager(prom_config),
    )


_DEP = DeploymentID(name="d", app_name="a")


class TestControllerScheduling:
    def test_no_schedule_without_config(self):
        stub = _controller_stub({})
        ServeController._maybe_schedule_prometheus_fetch(stub)
        assert stub._prometheus_fetch_task is None

    def test_no_schedule_within_interval(self):
        # A fetch time in the future keeps every tick inside the interval.
        stub = _controller_stub(
            {_DEP: (["q"], "addr")}, last_fetch_time=time.time() + 1e6
        )
        ServeController._maybe_schedule_prometheus_fetch(stub)
        assert stub._prometheus_fetch_task is None

    def test_no_schedule_while_in_flight(self):
        stub = _controller_stub({_DEP: (["q"], "addr")})
        stub._prometheus_fetch_task = types.SimpleNamespace(done=lambda: False)
        ServeController._maybe_schedule_prometheus_fetch(stub)
        # Existing task left untouched; no new one scheduled.
        assert stub._prometheus_fetch_task.done() is False

    def test_schedules_when_due(self):
        fetched = []

        async def fake_fetch(prom_config):
            fetched.append(prom_config)

        stub = _controller_stub({_DEP: (["q"], "addr")})
        stub._fetch_prometheus_metrics = fake_fetch

        async def run():
            ServeController._maybe_schedule_prometheus_fetch(stub)
            assert stub._prometheus_fetch_task is not None
            await stub._prometheus_fetch_task

        asyncio.run(run())
        assert fetched == [{_DEP: (["q"], "addr")}]
        assert stub._last_prometheus_fetch_time > 0


class TestControllerFetch:
    def test_aggregates_and_records(self, monkeypatch):
        d1 = DeploymentID(name="d1", app_name="a")
        d2 = DeploymentID(name="d2", app_name="a")
        prom_config = {d1: (["q1"], "addr1"), d2: (["q2"], "addr2")}
        stub = _controller_stub(prom_config)

        async def fake_fetch_metrics(session, address, queries):
            return {queries[0]: 1.0} if address == "addr1" else {}

        monkeypatch.setattr(controller_mod, "fetch_metrics", fake_fetch_metrics)
        monkeypatch.setattr(controller_mod.aiohttp, "ClientSession", _NoopSession)

        asyncio.run(ServeController._fetch_prometheus_metrics(stub, prom_config))
        recorded_metrics, _ = stub.autoscaling_state_manager.recorded
        # d2 returned nothing, so it is omitted from the recorded batch.
        assert recorded_metrics == {d1: {"q1": 1.0}}

    def test_per_deployment_error_is_swallowed(self, monkeypatch):
        prom_config = {_DEP: (["q1"], "addr1")}
        stub = _controller_stub(prom_config)

        async def boom(session, address, queries):
            raise RuntimeError("prometheus down")

        monkeypatch.setattr(controller_mod, "fetch_metrics", boom)
        monkeypatch.setattr(controller_mod.aiohttp, "ClientSession", _NoopSession)

        # Must not raise, and nothing is recorded.
        asyncio.run(ServeController._fetch_prometheus_metrics(stub, prom_config))
        assert stub.autoscaling_state_manager.recorded is None


class TestControllerShutdown:
    def test_cancels_in_flight_task(self):
        async def run():
            async def forever():
                await asyncio.sleep(100)

            task = asyncio.ensure_future(forever())
            await asyncio.sleep(0)
            stub = types.SimpleNamespace(_prometheus_fetch_task=task)
            ServeController._shutdown_prometheus_fetch(stub)
            assert stub._prometheus_fetch_task is None
            with pytest.raises(asyncio.CancelledError):
                await task

        asyncio.run(run())

    def test_noop_without_task(self):
        stub = types.SimpleNamespace(_prometheus_fetch_task=None)
        ServeController._shutdown_prometheus_fetch(stub)
        assert stub._prometheus_fetch_task is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
