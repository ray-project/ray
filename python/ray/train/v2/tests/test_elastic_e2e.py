import os
import sys
import time
from pathlib import Path
from typing import List

import pytest

import ray
import ray.train
from ray.cluster_utils import Cluster
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.jax import JaxTrainer


@pytest.fixture
def cluster():
    cluster = Cluster(initialize_head=True, head_node_args=dict(num_cpus=0))
    cluster.wait_for_nodes()
    ray.init(
        address=cluster.address,
        runtime_env={"working_dir": str(Path(__file__).parent)},
    )
    yield cluster
    ray.shutdown()
    cluster.shutdown()


def train_fn(config: dict):
    train_context = ray.train.get_context()
    rank = train_context.get_world_rank()

    start_epoch = 1
    checkpoint = ray.train.get_checkpoint()
    min_world_size = None
    max_world_size = None
    if checkpoint:
        checkpoint_data = load_dict_checkpoint(checkpoint)
        start_epoch = checkpoint_data["epoch"] + 1
        min_world_size = checkpoint_data.get("min_world_size")
        max_world_size = checkpoint_data.get("max_world_size")
        if rank == 0:
            print("Restoring from epoch: ", start_epoch)

    for epoch in range(start_epoch, config.get("num_epochs", 60) + 1):
        world_size = train_context.get_world_size()
        if min_world_size is None:
            min_world_size = world_size
        if max_world_size is None:
            max_world_size = world_size
        min_world_size = min(min_world_size, world_size)
        max_world_size = max(max_world_size, world_size)
        # TODO: This test injects errors by "killing nodes," which ungracefully
        # kills processes. This means that any backlog in the checkpoint queue
        # will not be flushed to the controller.
        # This means that the checkpoint populated on restore may not be
        # the most recent one.
        # Set the poll interval < health check interval to reduce the
        # backlog size to mitigate the issue.
        time.sleep(2 * config.get("health_check_interval_s", 1))

        with create_dict_checkpoint(
            {
                "epoch": epoch,
                "min_world_size": min_world_size,
                "max_world_size": max_world_size,
            }
        ) as checkpoint:
            ray.train.report(
                {
                    "epoch": epoch,
                    "world_size": world_size,
                    "min_world_size": min_world_size,
                    "max_world_size": max_world_size,
                },
                checkpoint=checkpoint if rank == 0 else None,
                checkpoint_dir_name=f"checkpoint-epoch={epoch}",
            )
        if rank == 0:
            print("Finished epoch: ", epoch)


def test_elastic_training(monkeypatch, tmp_path, cluster):
    """End to end test for elastic training.

    This test covers:
    * Elastic startup (0 resources -> min resources)
    * Elastic scale up while running (min resources -> max resources)
    * Elastic scale down due to failure while running
    * Checkpointing + restoration
    * Preemption failure handling
    """
    unit_time_s = 0.1
    health_check_interval_s = unit_time_s
    elastic_resize_monitor_interval_s = unit_time_s * 10
    num_epochs = 30

    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, str(health_check_interval_s))

    @ray.remote(num_cpus=0)
    def run_training():
        trainer = DataParallelTrainer(
            train_fn,
            train_loop_config={
                "num_epochs": num_epochs,
                "health_check_interval_s": health_check_interval_s,
            },
            scaling_config=ray.train.ScalingConfig(
                num_workers=(4, 32),
                use_gpu=True,
                elastic_resize_monitor_interval_s=elastic_resize_monitor_interval_s,
            ),
            run_config=ray.train.RunConfig(
                storage_path=str(tmp_path),
                checkpoint_config=ray.train.CheckpointConfig(num_to_keep=2),
                # NOTE: The outer test script will inject 2 node failures.
                failure_config=ray.train.FailureConfig(max_failures=2),
            ),
        )
        return trainer.fit()

    run_training_future = run_training.remote()

    start = time.time()
    ALL_NODES = []

    def print_status(message):
        elapsed = time.time() - start
        print()
        print("-" * 80)
        cluster_resources = {
            resource: value
            for resource, value in ray.cluster_resources().items()
            if resource in ("CPU", "GPU")
        }
        print(f"[elapsed={elapsed:.1f}s] {cluster_resources=}")
        print(message)
        print("-" * 80)
        print()

    def sleep(num_units):
        time.sleep(unit_time_s * num_units)

    def add_nodes(gpus: List[int]) -> List:
        added_nodes = []
        for num_gpus in gpus:
            node = cluster.add_node(num_gpus=num_gpus, wait=False)
            added_nodes.append(node)

        print_status(f"Added {len(gpus)} node(s) with num_gpus: {gpus}")
        cluster.wait_for_nodes()
        return added_nodes

    def remove_nodes(nodes: List):
        for node in nodes:
            cluster.remove_node(node)

        cluster.wait_for_nodes()
        print_status(f"Removed nodes: {nodes}")

    # Wait a bit before adding resources.
    print_status("Waiting for training to start...")
    sleep(8)

    # Add a node with 4 GPUs
    ALL_NODES.extend(add_nodes([4]))

    # Wait a bit before adding more resources.
    sleep(8)
    print("Adding 4 GPU node.")
    ALL_NODES.extend(add_nodes([4]))
    sleep(1)
    ALL_NODES.extend(add_nodes([4]))
    # Should not upscale here due to the elastic resize monitor interval.

    # Should upscale to 12 during this sleep.
    sleep(20)

    # Kill a node.
    remove_nodes([ALL_NODES.pop(0)])
    sleep(12)

    # Kill all worker nodes.
    remove_nodes(ALL_NODES)
    ALL_NODES = []

    sleep(8)
    ALL_NODES.extend(add_nodes(gpus=[1] * 16))

    sleep(12)
    # 4 extra GPUs shouldn't be used.
    ALL_NODES.extend(add_nodes(gpus=[4] * 4 + [1] * 4))

    result: ray.train.Result = ray.get(run_training_future)

    print_status(f"Training finished with result: {result}")
    assert not result.error
    assert result.metrics["min_world_size"] >= 4
    assert result.metrics["max_world_size"] <= 32
    assert result.metrics["max_world_size"] >= result.metrics["min_world_size"]
    assert result.checkpoint
    assert Path(result.checkpoint.path).name == f"checkpoint-epoch={num_epochs}"


def test_elastic_training_tpu(monkeypatch, tmp_path, cluster):
    """End to end test for TPU elastic training with the JaxTrainer."""
    unit_time_s = 1.0
    health_check_interval_s = unit_time_s
    elastic_resize_monitor_interval_s = unit_time_s * 5
    num_epochs = 30

    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, str(health_check_interval_s))
    monkeypatch.setenv("JAX_PLATFORMS", "cpu")

    @ray.remote(num_cpus=0)
    def run_training():
        trainer = JaxTrainer(
            train_fn,
            train_loop_config={
                "num_epochs": num_epochs,
                "health_check_interval_s": health_check_interval_s,
            },
            scaling_config=ray.train.ScalingConfig(
                use_tpu=True,
                accelerator_type="TPU-V6E",
                topology="2x4",
                resources_per_worker={"TPU": 4, "CPU": 1},
                num_workers=(2, 6),
                elastic_resize_monitor_interval_s=elastic_resize_monitor_interval_s,
            ),
            run_config=ray.train.RunConfig(
                storage_path=str(tmp_path),
                checkpoint_config=ray.train.CheckpointConfig(num_to_keep=2),
                failure_config=ray.train.FailureConfig(max_failures=3),
            ),
        )
        return trainer.fit()

    run_training_future = run_training.remote()

    start = time.time()
    ALL_NODES = []

    def print_status(message):
        elapsed = time.time() - start
        print(f"\n{'-' * 80}")
        cluster_resources = {
            resource: value
            for resource, value in ray.cluster_resources().items()
            if "TPU" in resource or "CPU" in resource
        }
        print(f"[elapsed={elapsed:.1f}s] {cluster_resources=}")
        print(message)
        print(f"{'-' * 80}\n")

    def wait_for_tpus(expected_tpus: int, timeout_s=45):
        start_wait = time.time()
        while time.time() - start_wait < timeout_s:
            current_tpus = ray.cluster_resources().get("TPU", 0)
            if current_tpus == expected_tpus:
                print_status(
                    f"Successfully registered {expected_tpus} TPUs in cluster."
                )
                return
            time.sleep(0.5)
        raise TimeoutError(
            f"Cluster failed to reach {expected_tpus} TPUs within {timeout_s}s."
        )

    def provision_tpu_node(slice_name: str, worker_id: int, is_head: bool = False):
        pod_type = "v6e-8"
        topology = "2x4"

        node_env = os.environ.copy()
        node_env["TPU_NAME"] = slice_name
        node_env["TPU_WORKER_ID"] = str(worker_id)
        node_env["TPU_ACCELERATOR_TYPE"] = pod_type
        node_env["TPU_TOPOLOGY"] = topology

        labels = {
            "ray.io/tpu-slice-name": slice_name,
            "ray.io/tpu-worker-id": str(worker_id),
            "ray.io/tpu-pod-type": pod_type,
        }

        resources = {"TPU": 4, "accelerator_type:TPU-V6E": 1}

        if is_head:
            resources[f"TPU-{pod_type}-head"] = 1

        node = cluster.add_node(
            num_cpus=8,
            resources=resources,
            labels=labels,
            env_vars=node_env,
            wait=False,
        )
        return node

    def remove_nodes(nodes: List):
        for node in nodes:
            cluster.remove_node(node)
        cluster.wait_for_nodes()
        print_status(f"Removed {len(nodes)} node(s).")

    print_status(
        "Adding 1 TPU node. Waiting for training to ignore it since it's not a full slice."
    )
    ALL_NODES.append(
        provision_tpu_node(slice_name="slice-A", worker_id=0, is_head=True)
    )
    wait_for_tpus(4)
    time.sleep(2)

    print_status("Adding 2nd TPU node to complete slice-A. Training should start.")
    ALL_NODES.append(
        provision_tpu_node(slice_name="slice-A", worker_id=1, is_head=False)
    )
    wait_for_tpus(8)

    time.sleep(12)

    print_status("Adding full second TPU slice. Policy should upscale.")
    ALL_NODES.append(
        provision_tpu_node(slice_name="slice-B", worker_id=0, is_head=True)
    )
    ALL_NODES.append(
        provision_tpu_node(slice_name="slice-B", worker_id=1, is_head=False)
    )
    wait_for_tpus(16)

    time.sleep(12)

    # Multi-host TPUs on GKE with KubeRay are scaled atomically in slices.
    print_status("Killing second TPU slice to simulate full slice preemption.")
    node_b_worker = ALL_NODES.pop()
    node_b_head = ALL_NODES.pop()
    remove_nodes([node_b_worker, node_b_head])
    wait_for_tpus(8)

    # Wait for policy to scale down to group with 2 workers.
    time.sleep(12)

    result: ray.train.Result = ray.get(run_training_future)

    print_status(f"Training finished with result: {result}")
    assert not result.error
    assert result.metrics["min_world_size"] == 2
    assert result.metrics["max_world_size"] == 4
    assert result.checkpoint
    assert Path(result.checkpoint.path).name == f"checkpoint-epoch={num_epochs}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
