"""End-to-end tests for the rate-based cluster autoscaler.

`test_rate_based_cluster_autoscaler.py` drives the autoscaler with stub operators.
These tests run a real dataset instead, so throughput rates, resource requirements,
and bundle counts are computed from actual `PhysicalOperator`s and `OpRuntimeMetrics`.

These live in their own module because several tests in the unit test file implicitly
initialize Ray, which conflicts with the cluster fixture used here.
"""

import logging
import time

import numpy as np
import pytest

import ray
from ray.data._internal.cluster_autoscaler import (
    CLUSTER_AUTOSCALER_ENV_KEY,
    DefaultClusterAutoscalerV2,
    RateBasedClusterAutoscaler,
    rate_based_cluster_autoscaler,
)
from ray.data._internal.cluster_autoscaler.default_cluster_autoscaler_v2 import (
    _NodeResourceSpec,
)
from ray.data._internal.cluster_autoscaler.resource_utilization_gauge import (
    RollingLogicalUtilizationGauge,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data._internal.util import GiB, MiB


@pytest.fixture(scope="module", autouse=True)
def shutdown_preexisting_ray():
    """Release any Ray instance an earlier test module left running.

    `test_rate_based_cluster_autoscaler.py` initializes Ray as a side effect of
    constructing autoscalers, which would make the cluster fixture here fail with
    "Maybe you called ray.init twice by accident?" when both modules run in one
    process. Bazel gives each target its own process, but a local
    `pytest python/ray/data/tests/` does not.
    """
    if ray.is_initialized():
        ray.shutdown()
    yield


@pytest.fixture
def recorded_resource_requests(monkeypatch):
    """Select the rate-based autoscaler and record every request it sends.

    Two defaults would otherwise stop a short test from ever reaching the scaling
    logic: requests are throttled to one per 10s, and utilization is averaged over a
    10s window that starts at zero, so the 50% scale-up gate is never cleared.
    Neither is reachable through the environment once this module is imported, since
    both are bound to constructor defaults at class-definition time, so they're
    shrunk by wrapping the constructor.
    """
    monkeypatch.setenv(CLUSTER_AUTOSCALER_ENV_KEY, "RATE_BASED")

    def _instantaneous_gauge(resource_manager, **kwargs):
        kwargs["cluster_util_avg_window_s"] = 0.01
        return RollingLogicalUtilizationGauge(resource_manager, **kwargs)

    monkeypatch.setattr(
        rate_based_cluster_autoscaler,
        "RollingLogicalUtilizationGauge",
        _instantaneous_gauge,
    )

    original_init = RateBasedClusterAutoscaler.__init__

    def _unthrottled_init(self, *args, **kwargs):
        kwargs["min_gap_between_autoscaling_requests_s"] = 0
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(RateBasedClusterAutoscaler, "__init__", _unthrottled_init)

    requests = []
    original_send = RateBasedClusterAutoscaler._send_resource_request

    def _record(self, resource_request):
        # `None` is a keep-alive that renews the registration without demand.
        requests.append(
            None
            if resource_request is None
            else [dict(bundle) for bundle in resource_request]
        )
        return original_send(self, resource_request)

    monkeypatch.setattr(RateBasedClusterAutoscaler, "_send_resource_request", _record)

    return requests


def _run_slow_dataset(num_rows: int = 1000, num_blocks: int = 20) -> int:
    """Run a dataset slow enough for cluster utilization to clear the scale-up gate."""

    def slow(row):
        time.sleep(0.05)
        return row

    ds = ray.data.range(num_rows, override_num_blocks=num_blocks).map(slow, num_cpus=1)
    # Iterate rather than call `count()`, which answers from block metadata without
    # running the plan, so no autoscaler would ever be created.
    return sum(1 for _ in ds.iter_rows())


def _run_shuffle_of_large_blocks(num_rows: int = 300, num_blocks: int = 12) -> int:
    """Run a sort over blocks big enough to saturate the object store budget."""

    def inflate(row):
        time.sleep(0.02)
        row["blob"] = np.zeros(256 * 1024, dtype=np.int8)
        return row

    ds = (
        ray.data.range(num_rows, override_num_blocks=num_blocks)
        .map(inflate, num_cpus=1)
        .sort("id")
    )
    return sum(1 for _ in ds.iter_rows())


def test_requests_resources_from_real_operator_metrics(
    ray_start_10_cpus_shared, recorded_resource_requests
):
    """The autoscaler requests resources driven by a real execution's throughput."""
    assert _run_slow_dataset() == 1000

    non_empty = [request for request in recorded_resource_requests if request]
    assert non_empty, (
        "The autoscaler never issued a non-empty resource request, so the scaling "
        "logic never ran against real operator metrics."
    )

    for request in non_empty:
        for bundle in request:
            # Ray can't satisfy an empty or non-positive bundle, and object store
            # memory can't be requested directly.
            assert bundle, request
            assert all(value > 0 for value in bundle.values()), bundle
            assert "object_store_memory" not in bundle, bundle

        # The map tasks each ask for 1 CPU, so every request must carry CPU bundles.
        assert any("CPU" in bundle for bundle in request), request


def test_cancels_its_request_when_execution_finishes(
    ray_start_10_cpus_shared,
    recorded_resource_requests,
    monkeypatch,
    propagate_logs,
    caplog,
):
    """The autoscaler releases the resources it asked for once the dataset finishes.

    Otherwise a finished dataset would hold a scale-up request until it expires,
    keeping nodes alive that nothing is using.
    """
    cancelled = []
    original_cancel = RateBasedClusterAutoscaler.on_executor_shutdown

    def _record_cancel(self):
        cancelled.append(True)
        return original_cancel(self)

    monkeypatch.setattr(
        RateBasedClusterAutoscaler, "on_executor_shutdown", _record_cancel
    )

    with caplog.at_level(
        logging.WARNING, logger="ray.data._internal.cluster_autoscaler"
    ):
        assert _run_slow_dataset(num_rows=200, num_blocks=10) == 200

    assert cancelled, "The autoscaler never cancelled its resource request."
    # `on_executor_shutdown` swallows failures and warns, so asserting it ran isn't
    # enough to know the request was actually released.
    assert "Failed to cancel resource request" not in caplog.text, caplog.text


def test_pads_request_with_cpu_bundles_when_object_store_is_full(
    ray_start_10_cpus_shared,
    recorded_resource_requests,
    restore_data_context,
    monkeypatch,
):
    """A saturated object store plus a shuffle makes the autoscaler ask for more CPUs.

    Ray offers no way to request object store memory directly, so the autoscaler asks
    for it indirectly by padding the request with logical CPUs. That branch needs both
    an incomplete all-to-all operator and object store utilization above the gate, so
    the other tests here never reach it.
    """
    # Utilization is usage over this limit, so a small budget saturates quickly and
    # avoids having to fill a real cluster's object store.
    restore_data_context.execution_options.resource_limits = (
        ExecutionResources.for_limits(object_store_memory=32 * MiB)
    )

    padded = []
    original_pad = (
        RateBasedClusterAutoscaler._pad_resource_request_for_object_store_memory
    )

    def _record_pad(self, resource_request, **kwargs):
        before = len(resource_request)
        original_pad(self, resource_request, **kwargs)
        padded.append(len(resource_request) - before)

    monkeypatch.setattr(
        RateBasedClusterAutoscaler,
        "_pad_resource_request_for_object_store_memory",
        _record_pad,
    )

    assert _run_shuffle_of_large_blocks() == 300

    assert padded, (
        "The autoscaler never padded a request, so the object store memory branch "
        "never ran."
    )
    assert any(
        count > 0 for count in padded
    ), f"Padding ran but added no CPU bundles: {padded}"


# Ray Data shouldn't scale up while blocked on a slow consumer. These tests also
# run with V2, which uses the same utilization gauge.
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


def _scale_up_stops(ds, scale_up_requests, quiet_s=3, timeout_s=30):
    """Read one batch from `ds` and pause. Return whether scale-up requests stop
    for `quiet_s` seconds within `timeout_s` seconds. Requests while the pipeline
    fills up are fine."""
    it = iter(ds.iter_batches(batch_size=None, prefetch_batches=0))
    next(it)
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
    # The consumer stays paused while the tasks wait in output backpressure.
    assert _scale_up_stops(ds, scale_up_requests)


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
    assert _scale_up_stops(ds, scale_up_requests)


def test_no_scale_up_while_consumer_is_slow_after_shuffle(
    ray_start_10_cpus_shared, restore_data_context, scale_up_requests
):
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.resource_limits = ExecutionResources.for_limits(
        object_store_memory=20 * MiB
    )

    # The finished shuffle's outputs wait in front of a map blocked on the consumer.
    ds = (
        ray.data.range(16, override_num_blocks=16)
        .map_batches(
            lambda batch: {"data": [b"x" * (2 * MiB) for _ in batch["id"]]},
            batch_size=1,
            num_cpus=0.5,
        )
        .random_shuffle()
        .map_batches(lambda batch: batch, num_cpus=1, concurrency=1)
    )
    assert _scale_up_stops(ds, scale_up_requests)


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
