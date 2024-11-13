import time

import numpy as np
import pytest

import ray
from ray.cluster_utils import Cluster
from ray.data import DataContext


@pytest.fixture
def cluster():
    cluster = Cluster(initialize_head=True, head_node_args=dict(num_cpus=0))
    cluster.wait_for_nodes()
    cluster.connect()
    yield cluster
    ray.shutdown()
    cluster.shutdown()


ROW_SIZE = 1024**2
MAP_SLEEP_DURATION_S = 0.001


def gen_dataset(
    num_rows_per_dataset: int,
    cpu_limit: int,
    gpu_limit: int,
    object_store_limit: int,
):
    context = DataContext.get_current()
    context.execution_options.resource_limits.cpu = cpu_limit
    context.execution_options.resource_limits.gpu = gpu_limit
    context.execution_options.resource_limits.object_store_memory = object_store_limit

    ds = ray.data.range(num_rows_per_dataset, override_num_blocks=num_rows_per_dataset)

    def cpu_map(row):
        time.sleep(MAP_SLEEP_DURATION_S)
        row["data"] = np.zeros(ROW_SIZE, dtype=np.int8)
        return row

    ds = ds.map(cpu_map, num_cpus=1)

    class GpuMap:
        def __call__(self, row):
            time.sleep(MAP_SLEEP_DURATION_S)
            return row

    ds = ds.map(GpuMap, num_gpus=1, concurrency=gpu_limit)
    return ds


def test_multiple_concurrent_datasets(cluster):
    """Test multiple concurrent datasets with resource limits in the same
    cluster."""
    num_nodes = 4
    resources_per_node = {
        "num_cpus": 8,
        "num_gpus": 4,
        "object_store_memory": 500 * 1024**2,
    }

    for _ in range(num_nodes):
        cluster.add_node(**resources_per_node)
    cluster.wait_for_nodes()

    num_datasets = 4
    num_rows_per_dataset = 1000

    @ray.remote(num_cpus=0)
    def run_dataset():
        ds = gen_dataset(
            num_rows_per_dataset,
            resources_per_node["num_cpus"],
            resources_per_node["num_gpus"],
            resources_per_node["object_store_memory"],
        )
        for _ in ds.iter_rows():
            pass

    run_dataset_tasks = [run_dataset.remote() for _ in range(num_datasets)]
    ray.get(run_dataset_tasks)


def test_nested_datasets(cluster):
    """Test nested datasets with resource limits in the same cluster.
    This is a common use case for training, where the validation dataset is
    consumed in the middle of train dataset.
    """
    num_nodes = 4
    resources_per_node = {
        "num_cpus": 8,
        "num_gpus": 4,
        "object_store_memory": 500 * 1024**2,
    }
    batch_size = 10
    validate_per_batch = 1000

    for _ in range(num_nodes):
        cluster.add_node(**resources_per_node)
    cluster.wait_for_nodes()

    train_ds = gen_dataset(
        3000,
        resources_per_node["num_cpus"] * 3,
        resources_per_node["num_gpus"] * 3,
        resources_per_node["object_store_memory"] * 3,
    )
    val_ds = gen_dataset(
        1000,
        resources_per_node["num_cpus"],
        resources_per_node["num_gpus"],
        resources_per_node["object_store_memory"],
    )

    @ray.remote(num_cpus=0)
    def run_ds(ds):
        for _ in ds.iter_torch_batches(batch_size=batch_size):
            pass

    for batch_idx, _ in enumerate(train_ds.iter_torch_batches(batch_size=batch_size)):
        if (batch_idx + 1) % validate_per_batch == 0:
            # NOTE, if not using a remote task, the progress bars of these 2 datasets
            # will overlap in the console.
            ray.get(run_ds.remote(val_ds))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
