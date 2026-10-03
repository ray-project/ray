"""Tests for https://github.com/ray-project/ray/issues/45331: Ray Data shouldn't
scale up the cluster while the pipeline is backpressured by a slow consumer.
"""

import contextlib
import time
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.cluster_autoscaler import resource_utilization_gauge
from ray.data._internal.cluster_autoscaler.default_cluster_autoscaler_v2 import (
    DefaultClusterAutoscalerV2,
    _NodeResourceSpec,
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
def test_gauge_excludes_output_backpressured_usage(exclude, expected_cpu_util):
    resource_manager = MagicMock()
    resource_manager.get_global_limits.return_value = ExecutionResources(cpu=8)
    resource_manager.get_global_usage.return_value = ExecutionResources(cpu=8)
    resource_manager.get_global_usage_excluding_output_backpressure.return_value = (
        ExecutionResources(cpu=2)
    )

    with patch.object(resource_utilization_gauge, "Gauge") as mock_gauge_cls:
        gauge = RollingLogicalUtilizationGauge(
            resource_manager,
            execution_id="ds",
            exclude_output_backpressured_usage=exclude,
        )
        gauge.observe()

    assert gauge.get().cpu == pytest.approx(expected_cpu_util)
    # Exported metrics always report raw utilization.
    mock_gauge_cls.return_value.set.assert_any_call(100, tags={"dataset": "ds"})


@contextlib.contextmanager
def _record_scale_up_requests(node_spec: _NodeResourceSpec):
    """Make the autoscaler decide quickly, and record its scale-up requests."""
    requests = []
    orig_init = DefaultClusterAutoscalerV2.__init__
    orig_log = DefaultClusterAutoscalerV2._log_resource_request

    def fast_init(self, *args, **kwargs):
        kwargs["min_gap_between_autoscaling_requests_s"] = 0.2
        kwargs["cluster_util_avg_window_s"] = 1
        kwargs["get_node_counts"] = lambda: {node_spec: 1}
        orig_init(self, *args, **kwargs)

    def log(self, *args):
        requests.append(time.time())
        orig_log(self, *args)

    with patch.object(DefaultClusterAutoscalerV2, "__init__", fast_init):
        with patch.object(DefaultClusterAutoscalerV2, "_log_resource_request", log):
            yield requests


def test_no_scale_up_while_consumer_is_slow(
    ray_start_regular_shared, restore_data_context
):
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.resource_limits = ExecutionResources.for_limits(
        object_store_memory=20 * MiB
    )

    with _record_scale_up_requests(_NodeResourceSpec.of(cpu=8, mem=GiB)) as requests:
        ds = ray.data.range(200, override_num_blocks=40).map_batches(
            lambda batch: {"data": [b"x" * (2 * MiB) for _ in batch["id"]]},
            batch_size=1,
        )
        it = iter(ds.iter_batches(batch_size=1, prefetch_batches=0))
        next(it)
        # Wait for the pipeline to settle into backpressure, then stay paused.
        time.sleep(1.5)
        paused_at = time.time()
        time.sleep(1.5)
        del it

    assert [t for t in requests if t >= paused_at] == []


def test_scale_up_when_compute_bound(ray_start_regular_shared, restore_data_context):
    def slow_udf(batch):
        time.sleep(0.5)
        return batch

    with _record_scale_up_requests(_NodeResourceSpec.of(cpu=1, mem=GiB)) as requests:
        ds = ray.data.range(8, override_num_blocks=8).map_batches(
            slow_udf, batch_size=None, num_cpus=1
        )
        for _ in ds.iter_batches(batch_size=None):
            pass

    assert requests


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
