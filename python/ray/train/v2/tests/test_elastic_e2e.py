import itertools
import sys
import time
from pathlib import Path
from typing import List, Optional

import pytest

import ray
import ray.train
from ray._common.test_utils import wait_for_condition
from ray.cluster_utils import Cluster
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


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


PROGRESS_TRACKER_NAME = "elastic_e2e_progress_tracker"


@ray.remote(num_cpus=0)
class ProgressTracker:
    """Lets the test driver observe training progress and decide when it ends.

    The driver waits on the world size rank 0 reports instead of sleeping for
    fixed intervals, so each cluster change happens in the training state it is
    meant to exercise, however slow nodes are to start on the test machine.
    """

    def __init__(self):
        self._epoch = 0
        self._world_size = None
        self._final_epoch = None

    def record(self, epoch: int, world_size: int):
        self._epoch = epoch
        self._world_size = world_size

    def world_size(self) -> Optional[int]:
        return self._world_size

    def final_epoch(self) -> Optional[int]:
        return self._final_epoch

    def finish(self, num_more_epochs: int) -> int:
        # Ranks are at most one epoch ahead of rank 0's last record, so any
        # margin > 1 is an epoch that every rank has yet to finish.
        self._final_epoch = self._epoch + num_more_epochs
        return self._final_epoch


def train_fn(config: dict):
    train_context = ray.train.get_context()
    rank = train_context.get_world_rank()
    tracker = ray.get_actor(PROGRESS_TRACKER_NAME)

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

    for epoch in itertools.count(start_epoch):
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
            ray.get(tracker.record.remote(epoch, world_size))

        final_epoch = ray.get(tracker.final_epoch.remote())
        if final_epoch is not None and epoch >= final_epoch:
            break


def test_elastic_training(monkeypatch, tmp_path, cluster):
    """End to end test for elastic training.

    This test covers:
    * Elastic startup (0 resources -> min resources)
    * Elastic scale up while running (min resources -> max resources)
    * Elastic scale down due to failure while running
    * Checkpointing + restoration
    * Preemption failure handling
    """
    health_check_interval_s = 0.1
    elastic_resize_monitor_interval_s = 1

    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, str(health_check_interval_s))

    # Pin the tracker to the head node, which the test never removes.
    tracker = ProgressTracker.options(
        name=PROGRESS_TRACKER_NAME,
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False
        ),
    ).remote()

    @ray.remote(num_cpus=0)
    def run_training():
        trainer = DataParallelTrainer(
            train_fn,
            train_loop_config={"health_check_interval_s": health_check_interval_s},
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

    # Submitted while the head node is the only node, so it runs there.
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

    def wait_for_world_size(world_size: int):
        wait_for_condition(
            lambda: ray.get(tracker.world_size.remote()) == world_size,
            timeout=60,
        )
        print_status(f"Training is running with world size {world_size}.")

    def add_nodes(gpus: List[int]) -> List:
        added_nodes = []
        for num_gpus in gpus:
            node = cluster.add_node(num_gpus=num_gpus, wait=False)
            added_nodes.append(node)

        cluster.wait_for_nodes()
        print_status(f"Added {len(gpus)} node(s) with num_gpus: {gpus}")
        return added_nodes

    def remove_nodes(nodes: List):
        for node in nodes:
            cluster.remove_node(node)

        cluster.wait_for_nodes()
        print_status(f"Removed nodes: {nodes}")

    # Elastic startup: nothing can run until the first node joins.
    print_status("Waiting for training to start...")
    assert ray.get(tracker.world_size.remote()) is None
    ALL_NODES.extend(add_nodes([4]))
    wait_for_world_size(4)

    # Scale up while running.
    ALL_NODES.extend(add_nodes([4, 4]))
    wait_for_world_size(12)

    # Scale down after a node failure.
    remove_nodes([ALL_NODES.pop(0)])
    wait_for_world_size(8)

    # Lose every worker node, then recover on many smaller nodes.
    remove_nodes(ALL_NODES)
    ALL_NODES = add_nodes([2] * 8)
    wait_for_world_size(16)

    # Scale up to max_workers. The 4 extra GPUs shouldn't be used.
    ALL_NODES.extend(add_nodes([4] * 4 + [2] * 2))
    wait_for_world_size(32)

    final_epoch = ray.get(tracker.finish.remote(num_more_epochs=3))
    result: ray.train.Result = ray.get(run_training_future)

    print_status(f"Training finished with result: {result}")
    assert not result.error
    assert result.metrics["min_world_size"] == 4
    assert result.metrics["max_world_size"] == 32
    assert result.checkpoint
    assert Path(result.checkpoint.path).name == f"checkpoint-epoch={final_epoch}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
