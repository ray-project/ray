import sys
from unittest.mock import MagicMock

import pytest

import ray.serve._private.replica as replica_module
from ray.serve._private.common import (
    DIRECT_INGRESS_RUNNING_REQUESTS_KEY,
    RUNNING_REQUESTS_KEY,
    RequestMetadata,
)
from ray.serve._private.metrics_utils import InMemoryMetricsStore
from ray.serve._private.replica import ReplicaMetricsManager
from ray.serve.config import AutoscalingConfig


def _make_metadata(request_id: str, *, is_direct_ingress: bool) -> RequestMetadata:
    return RequestMetadata(
        request_id=request_id,
        internal_request_id=request_id,
        is_direct_ingress=is_direct_ingress,
    )


def _make_fake_direct_ingress_manager():
    """Minimal stand-in for a direct-ingress replica's metrics manager, exposing the
    real request accounting and autoscaling metric recording."""
    fake = MagicMock()
    fake._is_direct_ingress = True
    fake._autoscaling_config = AutoscalingConfig()
    fake._num_ongoing_requests = 0
    fake._num_direct_ingress_ongoing_requests = 0
    fake._ingress_metrics = {}
    fake._cached_metrics_enabled = True
    fake._custom_metrics_enabled = False
    fake._metrics_store = InMemoryMetricsStore()
    for name in (
        "_change_num_ongoing_requests",
        "inc_num_ongoing_requests",
        "dec_num_ongoing_requests",
        "should_collect_ongoing_requests",
        "_add_autoscaling_metrics_point_async",
    ):
        method = getattr(ReplicaMetricsManager, name)
        setattr(fake, name, method.__get__(fake, ReplicaMetricsManager))
    return fake


@pytest.mark.asyncio
@pytest.mark.parametrize("collect_on_handle", [True, False])
async def test_direct_ingress_replica_running_requests_metric(
    collect_on_handle, monkeypatch
):
    """Under handle collection, a direct-ingress replica records only the requests that
    reached it directly, because handles report the ones they route. Otherwise it
    records every running request."""
    monkeypatch.setattr(
        replica_module,
        "RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE",
        collect_on_handle,
    )
    fake = _make_fake_direct_ingress_manager()
    finished = _make_metadata("finished", is_direct_ingress=True)
    fake.inc_num_ongoing_requests(finished)
    fake.inc_num_ongoing_requests(_make_metadata("direct", is_direct_ingress=True))
    fake.inc_num_ongoing_requests(_make_metadata("routed", is_direct_ingress=False))
    fake.dec_num_ongoing_requests(finished)
    # One recording only: a second could share its timestamp on Windows' clock.
    await fake._add_autoscaling_metrics_point_async()
    recorded = {
        key: [point.value for point in series]
        for key, series in fake._metrics_store.data.items()
    }
    if collect_on_handle:
        assert recorded == {DIRECT_INGRESS_RUNNING_REQUESTS_KEY: [1]}
    else:
        assert recorded == {RUNNING_REQUESTS_KEY: [2]}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
