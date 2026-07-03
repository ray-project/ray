"""Core tests for Prometheus autoscaling metrics + co-located query cache."""

import asyncio
import sys
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from ray._common.prometheus_utils import (
    NodeLocalPrometheusQueryCache,
    extract_instant_query_value,
    normalize_prometheus_address,
    reset_default_prometheus_query_cache_for_tests,
)
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.replica import ReplicaMetricsManager
from ray.serve.config import AutoscalingConfig


def _rid(i: int):
    try:
        return ReplicaID(
            unique_id=f"replica-{i}",
            deployment_id=DeploymentID(name="D", app_name="A"),
        )
    except TypeError:

        class _Rid:
            unique_id = f"replica-{i}"
            deployment_id = DeploymentID(name="D", app_name="A")

        return _Rid()


def test_normalize_and_extract():
    assert normalize_prometheus_address("http://h:9") == "h:9"
    assert normalize_prometheus_address("https://secure:9090/path") == "secure:9090"
    assert (
        extract_instant_query_value({"data": {"result": [{"value": [1, "42"]}]}})
        == 42.0
    )
    assert (
        extract_instant_query_value(
            {"data": {"resultType": "scalar", "result": [1, "3"]}}
        )
        == 3.0
    )
    # Error payloads must not be treated as a numeric sample.
    assert (
        extract_instant_query_value(
            {"status": "error", "errorType": "bad_data", "error": "parse error"}
        )
        is None
    )


def test_fetch_raises_on_prometheus_status_error(monkeypatch):
    from ray._common.prometheus_utils import fetch_from_prom_server

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "error", "errorType": "bad_data", "error": "parse error"}

    monkeypatch.setattr(
        "ray._common.prometheus_utils.requests.get", lambda *a, **k: _Resp()
    )
    with pytest.raises(ValueError, match="Prometheus query error"):
        fetch_from_prom_server("localhost:9090", "up")


def test_zero_ttl_never_serves_cache_hits(tmp_path):
    calls = {"n": 0}

    def fetch(address, query, timeout=None):
        calls["n"] += 1
        return {"data": {"result": [{"value": [1, str(calls["n"])]}]}}

    cache = NodeLocalPrometheusQueryCache(ttl_s=0.0, cache_dir=str(tmp_path / "z"))
    cache.get_or_fetch("localhost:9090", "m", fetch_fn=fetch)
    cache.get_or_fetch("localhost:9090", "m", fetch_fn=fetch)
    assert calls["n"] == 2


def test_cache_colocated_dedup(tmp_path):
    calls = {"n": 0}

    def fetch(address, query, timeout=None):
        calls["n"] += 1
        time.sleep(0.01)
        return {"data": {"result": [{"value": [1, "7"]}]}}

    cache = NodeLocalPrometheusQueryCache(ttl_s=5.0, cache_dir=str(tmp_path / "c"))
    out = []

    def worker():
        out.append(cache.get_or_fetch("localhost:9090", "m", fetch_fn=fetch))

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert calls["n"] == 1
    assert len(out) == 4


@pytest.mark.asyncio
async def test_replica_fetch_value_and_colocated_dedup(tmp_path, monkeypatch):
    import ray.serve._private.replica as replica_mod

    calls = {"n": 0}

    def handler(host, query, timeout=None):
        calls["n"] += 1
        return {"data": {"result": [{"value": [1, "42"]}]}}

    monkeypatch.setattr(
        replica_mod,
        "RAY_SERVE_REPLICA_AUTOSCALING_METRIC_PROMETHEUS_HOST",
        "localhost:9090",
    )
    reset_default_prometheus_query_cache_for_tests(
        NodeLocalPrometheusQueryCache(ttl_s=5.0, cache_dir=str(tmp_path / "m"))
    )
    cfg = AutoscalingConfig(
        min_replicas=1,
        max_replicas=3,
        target_ongoing_requests=1,
        prometheus_metrics=["custom_load"],
    )
    with patch.object(ReplicaMetricsManager, "start_metrics_pusher"):
        with patch(
            "ray.serve._private.replica.ray.get_actor", return_value=MagicMock()
        ):
            managers = [
                ReplicaMetricsManager(
                    replica_id=_rid(i),
                    event_loop=asyncio.get_event_loop(),
                    autoscaling_config=cfg,
                    ingress=True,
                    max_ongoing_requests=10,
                    prometheus_handler=handler,
                )
                for i in range(3)
            ]
            results = [
                await m._fetch_prometheus_metrics(["custom_load"]) for m in managers
            ]
    assert all(r == {"custom_load": 42.0} for r in results)
    assert calls["n"] == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
