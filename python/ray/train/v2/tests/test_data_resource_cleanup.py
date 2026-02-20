import sys
import time
from unittest.mock import MagicMock, create_autospec

import pytest

import ray
from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
    get_or_create_autoscaling_coordinator,
)
from ray.data._internal.iterator.stream_split_iterator import (
    SplitCoordinator,
)
from ray.train.v2._internal.callbacks.datasets import DatasetsCallback
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroup,
    WorkerGroupContext,
)
from ray.train.v2.tests.util import DummyObjectRefWrapper, create_dummy_run_context

pytestmark = pytest.mark.usefixtures("mock_runtime_context")


def test_datasets_callback_multiple_datasets(ray_start_4_cpus):
    """Test that the DatasetsCallback properly collects the coordinator actors for multiple datasets"""
    # Start worker group
    worker_group_context = WorkerGroupContext(
        run_attempt_id="test",
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        num_workers=4,
        resources_per_worker={"CPU": 1},
    )
    wg = WorkerGroup.create(
        train_run_context=create_dummy_run_context(),
        worker_group_context=worker_group_context,
    )

    # Create train run context
    NUM_ROWS = 100

    datasets = {
        "sharded_1": ray.data.range(NUM_ROWS),
        "sharded_2": ray.data.range(NUM_ROWS),
        "unsharded": ray.data.range(NUM_ROWS),
    }
    dataset_config = ray.train.DataConfig(datasets_to_split=["sharded_1", "sharded_2"])
    train_run_context = create_dummy_run_context(
        datasets=datasets, dataset_config=dataset_config
    )

    callback = DatasetsCallback(train_run_context)
    callback.before_init_train_context(wg.get_workers())

    # Two coordinator actors, one for each sharded dataset
    coordinator_actors = callback._coordinator_actors
    assert len(coordinator_actors) == 2


def test_after_worker_group_abort():
    callback = DatasetsCallback(create_dummy_run_context())

    # Mock SplitCoordinator shutdown_executor method
    coord_mock = create_autospec(SplitCoordinator)
    remote_mock = MagicMock()
    coord_mock.shutdown_executor.remote = remote_mock
    callback._coordinator_actors = [coord_mock]

    dummy_wg_context = WorkerGroupContext(
        run_attempt_id="test",
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        num_workers=4,
        resources_per_worker={"CPU": 1},
    )
    callback.after_worker_group_abort(dummy_wg_context)

    # shutdown_executor called on SplitCoordinator
    remote_mock.assert_called_once()


def test_after_worker_group_shutdown():
    callback = DatasetsCallback(create_dummy_run_context())

    # Mock SplitCoordinator shutdown_executor method
    coord_mock = create_autospec(SplitCoordinator)
    remote_mock = MagicMock()
    coord_mock.shutdown_executor.remote = remote_mock
    callback._coordinator_actors = [coord_mock]

    dummy_wg_context = WorkerGroupContext(
        run_attempt_id="test",
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        num_workers=4,
        resources_per_worker={"CPU": 1},
    )
    callback.after_worker_group_shutdown(dummy_wg_context)

    # shutdown_executor called on SplitCoordinator
    remote_mock.assert_called_once()


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
