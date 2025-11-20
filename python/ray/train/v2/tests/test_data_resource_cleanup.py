import sys

import pytest

import ray
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


@pytest.fixture
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


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


def test_split_coordinator_shutdown_executor(ray_start_4_cpus):
    """Tests that the SplitCoordinator properly requests resources for the data executor and cleans up after it is shutdown"""

    @ray.remote(num_cpus=0)
    class DummyRequester:
        def __init__(self):
            self.calls = []

        def request_resources(self, bundles, execution_id):
            self.calls.append((bundles, execution_id))

        def get_calls(self):
            return self.calls

        def clear(self):
            self.calls = []

    # Create the named requester actor that DefaultClusterAutoscaler will find.
    dummy_requester = DummyRequester.options(
        name="AutoscalingRequester",
        namespace="AutoscalingRequester",
        lifetime="detached",
        get_if_exists=True,
    ).remote()
    ray.get(dummy_requester.clear.remote())

    # Start coordinator and executor
    NUM_SPLITS = 1
    dataset = ray.data.range(100)
    coord = SplitCoordinator.options(name="test_split_coordinator").remote(
        _DatasetWrapper(dataset), NUM_SPLITS, None
    )
    ray.get(coord.start_epoch.remote(0))
    calls = ray.get(dummy_requester.get_calls.remote())

    # Shutdown data executor
    ray.get(coord.shutdown_executor.remote())
    calls = ray.get(dummy_requester.get_calls.remote())

    # One request made for the data executor, one cleanup request
    assert len(calls) == 2
    # Cleanup request made for the data executor
    assert any(bundles == {} and isinstance(exec_id, str) for bundles, exec_id in calls)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
