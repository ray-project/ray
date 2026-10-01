import sys
import time
from unittest.mock import create_autospec

import pytest

import ray
from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
    get_or_create_autoscaling_coordinator,
)
from ray.data._internal.iterator.stream_split_iterator import (
    SplitCoordinator,
)
from ray.train.v2._internal.callbacks.datasets import (
    DatasetsCallback,
    RayDatasetShardProvider,
)
from ray.train.v2._internal.execution.worker_group import WorkerGroupContext
from ray.train.v2.tests.util import DummyObjectRefWrapper, create_dummy_run_context

pytestmark = pytest.mark.usefixtures("mock_runtime_context")


def _dummy_worker_group_context() -> WorkerGroupContext:
    return WorkerGroupContext(
        run_attempt_id="test",
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        num_workers=4,
        resources_per_worker={"CPU": 1},
    )


def test_after_worker_group_shutdown():
    """The callback delegates shutdown to the dataset shard provider."""
    callback = DatasetsCallback(
        train_run_context=create_dummy_run_context(), datasets={}
    )
    shard_provider = create_autospec(RayDatasetShardProvider)
    callback._dataset_shard_provider = shard_provider

    callback.after_worker_group_shutdown(
        worker_group_context=_dummy_worker_group_context()
    )
    shard_provider.shutdown_data_executors.assert_called_once()


def test_after_worker_group_abort():
    """The callback delegates abort cleanup to the dataset shard provider."""
    callback = DatasetsCallback(
        train_run_context=create_dummy_run_context(), datasets={}
    )
    shard_provider = create_autospec(RayDatasetShardProvider)
    callback._dataset_shard_provider = shard_provider

    callback.after_worker_group_abort(
        worker_group_context=_dummy_worker_group_context()
    )
    shard_provider.shutdown_data_executors.assert_called_once()


def test_split_coordinator_shutdown_executor(ray_start_4_cpus):
    """Tests that the SplitCoordinator properly requests resources for the data executor and cleans up after it is shutdown"""

    def get_ongoing_requests(coordinator, timeout=3.0):
        """Retrieve ongoing requests from the AutoscalingCoordinator."""
        deadline = time.time() + timeout
        requests = {}
        while time.time() < deadline:
            requests = ray.get(
                coordinator.__ray_call__.remote(lambda c: dict(c._ongoing_reqs))
            )
            if requests:
                break
            time.sleep(0.05)
        return requests

    # Start coordinator and executor
    NUM_SPLITS = 1
    dataset = ray.data.range(100)
    coord = SplitCoordinator.options(name="test_split_coordinator").remote(
        dataset, NUM_SPLITS, None
    )
    ray.get(coord.start_epoch.remote(0))

    # Explicitly trigger autoscaling
    ray.get(
        coord.__ray_call__.remote(
            lambda coord: coord._current_executor._cluster_autoscaler.try_trigger_scaling()
        )
    )

    # Collect requests from the AutoscalingCoordinator
    coordinator = get_or_create_autoscaling_coordinator()
    requests = get_ongoing_requests(coordinator)

    # One request made (V2 registers with the coordinator)
    assert len(requests) == 1
    requester_id = list(requests.keys())[0]
    assert requester_id.startswith("data-")

    # Shutdown data executor
    ray.get(coord.shutdown_executor.remote())

    # Verify that the request is cancelled (removed from ongoing requests)
    requests = ray.get(coordinator.__ray_call__.remote(lambda c: dict(c._ongoing_reqs)))
    assert len(requests) == 0, "Resource request was not cancelled"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
