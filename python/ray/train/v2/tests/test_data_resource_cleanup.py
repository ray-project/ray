import sys
import time
from unittest.mock import MagicMock, create_autospec

import pytest

import ray
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
)
from ray.data._internal.iterator.stream_split_iterator import (
    SplitCoordinator,
    _DatasetWrapper,
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

    def get_resources_when_updated(requester, prev_requests=None, timeout=3.0):
        """Retrieve resource requests within the specified timeout. Returns after a new request is made or when the time expires."""
        prev_requests = prev_requests or {}
        deadline = time.time() + timeout
        requests = {}
        while time.time() < deadline:
            requests = ray.get(
                requester.__ray_call__.remote(lambda r: r._resource_requests)
            )
            if requests != prev_requests:
                break
            time.sleep(0.05)
        return requests

    # Start coordinator and executor
    NUM_SPLITS = 1
    dataset = ray.data.range(100)
    coord = SplitCoordinator.options(name="test_split_coordinator").remote(
        _DatasetWrapper(dataset), NUM_SPLITS, None
    )
    ray.get(coord.start_epoch.remote(0))

    # Explicity trigger autoscaling
    ray.get(
        coord.__ray_call__.remote(
            lambda coord: coord._executor._cluster_autoscaler.try_trigger_scaling()
        )
    )

    # Collect requests
    requester = get_or_create_autoscaling_requester_actor()
    requests = get_resources_when_updated(requester)

    # One request made, with non-empty resource bundle
    assert len(requests) == 1
    resource_bundles = list(requests.values())[0][0]
    assert isinstance(resource_bundles, list)
    bundle = resource_bundles[0]
    assert bundle != {}

    # Shutdown data executor
    ray.get(coord.shutdown_executor.remote())

    requests = get_resources_when_updated(requester, prev_requests=requests)

    # Old resource request overwritten by new cleanup request
    assert len(requests) == 1
    resource_bundles = list(requests.values())[0][0]
    assert isinstance(resource_bundles, dict)
    assert resource_bundles == {}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
