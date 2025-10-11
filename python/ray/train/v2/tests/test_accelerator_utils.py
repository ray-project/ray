import collections
import os

import pytest

import ray
from ray.cluster_utils import Cluster
from ray.train import BackendConfig
from ray.train.backend import Backend
from ray.train.v2._internal.callbacks.accelerators import (
    AcceleratorSetupCallback,
    _get_visible_accelerator_ids_per_worker,
)
from ray.train.v2._internal.execution.worker_group import ActorMetadata, WorkerGroup
from ray.train.v2._internal.execution.worker_group.worker_group import (
    WorkerGroupContext,
)
from ray.train.v2._internal.util import ObjectRefWrapper
from ray.train.v2.api.config import ScalingConfig
from ray.train.v2.tests.util import create_dummy_run_context


@pytest.fixture
def mock_gpu_cluster():
    """Yields a GPU cluster with 3 nodes (4 GPU, 1 GPU, 1 GPU)."""
    cluster = Cluster()
    cluster.add_node(num_gpus=4)
    cluster.add_node(num_gpus=1)
    cluster.add_node(num_gpus=1)
    cluster.wait_for_nodes()
    cluster.connect()
    yield cluster
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.parametrize(
    "node_ids, accelerator_ids_per_worker, expected",
    [
        (["0"], [[0]], ["0"]),
        (
            ["0", "0", "1"],
            [[0, 1], [2, 3], [0, 1]],
            ["0,1,2,3", "0,1,2,3", "0,1"],
        ),
        (
            ["0", "0", "1", "1", "1", "1"],
            [["1"], ["3"], ["3"], ["0"], ["1"], ["2"]],
            ["1,3", "1,3", "0,1,2,3", "0,1,2,3", "0,1,2,3", "0,1,2,3"],
        ),
    ],
)
def test_get_visible_accelerator_ids_per_worker(
    node_ids, accelerator_ids_per_worker, expected
):
    worker_metadatas = [
        ActorMetadata(
            hostname=node_id,
            node_id=node_id,
            node_ip=node_id,
            pid=0,
            accelerator_ids={"GPU": accelerator_ids},
        )
        for node_id, accelerator_ids in zip(node_ids, accelerator_ids_per_worker)
    ]

    assert (
        _get_visible_accelerator_ids_per_worker(
            worker_metadatas=worker_metadatas, accelerator_name="GPU"
        )
        == expected
    )


def test_missing_accelerator():
    """Trying to share accelerator ids on a heterogeneous worker group
    (where some workers do not have access to certain accelerators)
    should raise an error."""
    with pytest.raises(ValueError):
        _get_visible_accelerator_ids_per_worker(
            worker_metadatas=[
                ActorMetadata(
                    hostname="0",
                    node_id="0",
                    node_ip="0",
                    pid=0,
                    accelerator_ids={"GPU": [0]},
                ),
                ActorMetadata(
                    hostname="0",
                    node_id="0",
                    node_ip="0",
                    pid=0,
                    accelerator_ids={},
                ),
            ],
            accelerator_name="GPU",
        )


def test_accelerator_setup_callback(mock_gpu_cluster, mock_runtime_context):
    """The accelerator setup callback should set the CUDA_VISIBLE_DEVICES
    on each worker properly."""

    class DummyBackendConfig(BackendConfig):
        def backend_cls(self):
            return DummyBackend

    class DummyBackend(Backend):
        share_cuda_visible_devices = True

    scaling_config = ScalingConfig(num_workers=6, use_gpu=True)
    setup_callback = AcceleratorSetupCallback(
        backend_config=DummyBackendConfig(),
        scaling_config=scaling_config,
    )

    worker_group_context = WorkerGroupContext(
        run_attempt_id="attempt_1",
        train_fn_ref=ObjectRefWrapper(lambda: None),
        num_workers=scaling_config.num_workers,
        resources_per_worker=scaling_config._resources_per_worker_not_none,
    )

    worker_group = WorkerGroup(
        train_run_context=create_dummy_run_context(),
        worker_group_context=worker_group_context,
    )

    worker_group._start()

    setup_callback.before_init_train_context(worker_group.get_workers())

    visible_devices_per_worker = worker_group.execute(
        lambda: os.environ["CUDA_VISIBLE_DEVICES"]
    )
    assert collections.Counter(visible_devices_per_worker) == {"0,1,2,3": 4, "0": 2}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
