"""Tests that Ray Data doesn't scale up while blocked on a slow consumer (#45331)."""

import time
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.cluster_autoscaler import (
    CLUSTER_AUTOSCALER_ENV_KEY,
    rate_based_cluster_autoscaler,
    resource_utilization_gauge,
)
from ray.data._internal.cluster_autoscaler.default_cluster_autoscaler_v2 import (
    DefaultClusterAutoscalerV2,
    _NodeResourceSpec,
)
from ray.data._internal.cluster_autoscaler.rate_based_cluster_autoscaler import (
    RateBasedClusterAutoscaler,
)
from ray.data._internal.cluster_autoscaler.resource_utilization_gauge import (
    RollingLogicalUtilizationGauge,
)
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionResources,
)
from ray.data._internal.util import GiB, MiB
from ray.data.tests.conftest import *  # noqa


@pytest.mark.parametrize("exclude, expected_cpu_util", [(True, 0.25), (False, 1.0)])
def test_gauge_excludes_consumer_blocked_usage(exclude, expected_cpu_util):
    resource_manager = MagicMock()
    resource_manager.get_global_limits.return_value = ExecutionResources(cpu=8)
    resource_manager.get_global_usage.return_value = ExecutionResources(cpu=8)
    resource_manager.get_global_usage_excluding_consumer_blocked_ops.return_value = (
        ExecutionResources(cpu=2)
    )

    with patch.object(resource_utilization_gauge, "Gauge") as mock_gauge_cls:
        gauge = RollingLogicalUtilizationGauge(
            resource_manager,
            execution_id="ds",
            exclude_consumer_blocked_usage=exclude,
        )
        gauge.observe()

    assert gauge.get().cpu == pytest.approx(expected_cpu_util)
    # Exported metrics always report raw utilization.
    mock_gauge_cls.return_value.set.assert_any_call(100, tags={"dataset": "ds"})


@pytest.fixture(params=["V2", "RATE_BASED"])
def scale_up_requests(request, monkeypatch):
    """Use the given autoscaler, make it decide fast, and record scale-up times."""
    monkeypatch.setenv(CLUSTER_AUTOSCALER_ENV_KEY, request.param)
    requests = []

    if request.param == "V2":
        orig_init = DefaultClusterAutoscalerV2.__init__
        orig_log = DefaultClusterAutoscalerV2._log_resource_request

        def fast_init(self, *args, **kwargs):
            kwargs["min_gap_between_autoscaling_requests_s"] = 0.2
            kwargs["cluster_util_avg_window_s"] = 1
            # A local cluster has no worker node shapes to request more of.
            node_spec = _NodeResourceSpec.of(cpu=1, mem=GiB)
            kwargs["get_node_counts"] = lambda: {node_spec: 1}
            orig_init(self, *args, **kwargs)

        # V2 only logs a request that adds nodes.
        def log(self, *args):
            requests.append(time.time())
            orig_log(self, *args)

        monkeypatch.setattr(DefaultClusterAutoscalerV2, "__init__", fast_init)
        monkeypatch.setattr(DefaultClusterAutoscalerV2, "_log_resource_request", log)
    else:
        orig_init = RateBasedClusterAutoscaler.__init__
        orig_send = RateBasedClusterAutoscaler._send_resource_request

        def fast_gauge(resource_manager, **kwargs):
            kwargs["cluster_util_avg_window_s"] = 1
            return RollingLogicalUtilizationGauge(resource_manager, **kwargs)

        def fast_init(self, *args, **kwargs):
            kwargs["min_gap_between_autoscaling_requests_s"] = 0.2
            orig_init(self, *args, **kwargs)

        # Rate-based sends `None` when utilization is too low to scale up.
        def send(self, resource_request):
            if resource_request:
                requests.append(time.time())
            return orig_send(self, resource_request)

        monkeypatch.setattr(
            rate_based_cluster_autoscaler, "RollingLogicalUtilizationGauge", fast_gauge
        )
        monkeypatch.setattr(RateBasedClusterAutoscaler, "__init__", fast_init)
        monkeypatch.setattr(RateBasedClusterAutoscaler, "_send_resource_request", send)

    return requests


def _scale_up_stops(scale_up_requests, quiet_s=3, timeout_s=30):
    """Return whether scale-up requests stop for `quiet_s` seconds within
    `timeout_s` seconds. Requests while the pipeline fills up are fine."""
    start = time.time()
    while time.time() - start < timeout_s:
        if time.time() - max([start, *scale_up_requests]) >= quiet_s:
            return True
        time.sleep(0.1)
    return False


def test_no_scale_up_while_consumer_is_slow(
    ray_start_10_cpus_shared, restore_data_context, scale_up_requests
):
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.resource_limits = ExecutionResources.for_limits(
        object_store_memory=20 * MiB
    )

    ds = ray.data.range(200, override_num_blocks=40).map_batches(
        lambda batch: {"data": [b"x" * (2 * MiB) for _ in batch["id"]]},
        batch_size=1,
    )
    it = iter(ds.iter_batches(batch_size=1, prefetch_batches=0))
    next(it)
    # The consumer stays paused while the tasks wait in output backpressure.
    stopped = _scale_up_stops(scale_up_requests)
    del it

    assert stopped


def test_no_scale_up_while_consumer_is_slow_two_ops(
    ray_start_10_cpus_shared, restore_data_context, scale_up_requests
):
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.resource_limits = ExecutionResources.for_limits(
        object_store_memory=20 * MiB
    )

    # One task at a time, so tasks finish and both ops sit idle with queued outputs.
    ds = (
        ray.data.from_items(list(range(200)), override_num_blocks=200).map_batches(
            lambda batch: {"data": [b"x" * MiB for _ in batch["item"]]},
            num_cpus=0.5,
            concurrency=1,
        )
        # A different `num_cpus` keeps the two maps from being fused.
        .map_batches(lambda batch: batch, num_cpus=1, concurrency=1)
    )
    it = iter(ds.iter_batches(batch_size=None, prefetch_batches=0))
    next(it)
    stopped = _scale_up_stops(scale_up_requests)
    del it

    assert stopped


def test_scale_up_when_compute_bound(
    ray_start_10_cpus_shared, restore_data_context, scale_up_requests
):
    def slow_udf(batch):
        time.sleep(1)
        return batch

    # 40 tasks keep the 10 CPUs busy for about 4 seconds.
    ds = ray.data.range(40, override_num_blocks=40).map_batches(
        slow_udf, batch_size=None, num_cpus=1
    )
    for _ in ds.iter_batches(batch_size=None):
        pass

    assert scale_up_requests


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
