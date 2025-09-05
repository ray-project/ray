"""
This test suite covers error handling and propagation in Ray Train/Tune.

There are two main error types to test:
1. Trainable errors: These happen in the remote actor itself.
    -> Within this, we should test:
        - fail_fast=True/False/'raise'
        - AIR Trainer w/o Tuner, AIR Trainer w/ Tuner, Tuner w/ function trainable
2. Tune driver errors: These happen in the Tune event-handling loop.
    -> Within this, we should test:
        - Errors occurring at different points in the Tune loop
          (on_trial_result, on_checkpoint, on_step_begin, etc.)

These tests should:
- Assert how errors from the trainable/Trainer get propagated to the user.
- Assert how errors from the Tune driver get propagated to the user.
"""

import gc
import threading
import time
from tempfile import TemporaryDirectory

import pytest

import ray
from ray import train, tune
from ray._common.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.cluster_utils import Cluster
from ray.core.generated import autoscaler_pb2
from ray.tests.conftest import *  # noqa
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
from ray.train.trainer import BaseTrainer, TrainingFailedError
from ray.tune import TuneError, Tuner


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4, configure_logging=False)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture(autouse=True)
def gc_collect():
    # Make sure to cleanup as much as possible between
    # unit tests that share a Ray session
    yield
    gc.collect()


@pytest.fixture
def cluster_setup(ray_start_cluster_head: Cluster):
    # Sets up a cluster with 3 nodes: head node + 2 workers
    cluster = ray_start_cluster_head
    nodes = []
    nodes.append(cluster.add_node(resources={"worker1": 1, "cpu": 1, "coordinator": 1}))
    nodes.append(cluster.add_node(resources={"worker2": 1, "cpu": 1}))
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    worker1_node_id = ray.get(get_node_id.options(resources={"worker1": 1}).remote())
    worker2_node_id = ray.get(get_node_id.options(resources={"worker2": 1}).remote())
    wait_for_condition(
        lambda: len({node["NodeID"] for node in ray.nodes() if (node["Alive"])}) == 3
    )

    yield cluster, nodes, [
        worker1_node_id,
        worker2_node_id,
    ]


class _TestSpecificError(RuntimeError):
    pass


class FailingCallback(tune.Callback):
    def __init__(self, error_on: str):
        self.error_on = error_on

    def on_trial_result(self, *args, **kwargs):
        if self.error_on == "on_trial_result":
            raise _TestSpecificError(f"Failing on {self.error_on}!")


class FailingTrainer(BaseTrainer):
    def training_loop(self) -> None:
        raise _TestSpecificError("There is an error in trainer!")


def passing_fn(config):
    # Trigger all the driver events (on_checkpoint, on_trial_save, etc.)
    with TemporaryDirectory() as tmpdir:
        train.report({"score": 1}, checkpoint=train.Checkpoint.from_directory(tmpdir))


def failing_fn(config):
    raise _TestSpecificError("Failing!")


trainable_map = {
    "function": failing_fn,
    "trainer": FailingTrainer(),
}


@pytest.mark.parametrize("fail_fast", [False, True, "raise"])
def test_trainable_error_with_tuner(ray_start_4_cpus, fail_fast):
    tuner = Tuner(
        trainable=failing_fn,
        run_config=tune.RunConfig(
            name=f"tuner_errors-fail_fast={fail_fast}",
            failure_config=tune.FailureConfig(fail_fast=fail_fast),
        ),
        tune_config=tune.TuneConfig(num_samples=2),
    )

    if fail_fast is False:
        # Both trials should complete with an error.
        results = tuner.fit()
        assert len(results) == 2
        for i in range(2):
            assert results[i].error
    elif fail_fast is True:
        # The first trial errors -> the experiment finishes immediately.
        results = tuner.fit()
        errors = [result.error for result in results if result.error]
        assert len(errors) == 1
    elif fail_fast == "raise":
        # The original error gets raised to the user
        with pytest.raises(_TestSpecificError):
            tuner.fit()


@pytest.mark.parametrize("fail_fast", [False, True, "raise"])
def test_trainable_error_with_trainer(ray_start_4_cpus, tmp_path, fail_fast):
    name = f"test_trainer_errors-fail_fast={fail_fast}"
    trainer = FailingTrainer(
        run_config=train.RunConfig(
            storage_path=str(tmp_path),
            name=name,
            failure_config=train.FailureConfig(fail_fast=fail_fast),
        ),
        scaling_config=train.ScalingConfig(num_workers=1),
    )

    if fail_fast in [False, True]:
        # There is only 1 "trial" for a Trainer,
        # so fail_fast = True/False doesn't change the behavior
        # In both cases, the error should get wrapped and raised.
        with pytest.raises(TrainingFailedError) as exc_info:
            trainer.fit()

        # The cause of the error should be the trainable error
        assert isinstance(exc_info.value.__cause__, _TestSpecificError)

        assert TrainingFailedError._RESTORE_MSG.format(
            trainer_cls_name="FailingTrainer", path=str(tmp_path / name)
        ) in str(exc_info.value)
        assert TrainingFailedError._FAILURE_CONFIG_MSG in str(exc_info.value)

    elif fail_fast == "raise":
        # The original error gets raised to the user
        with pytest.raises(_TestSpecificError):
            trainer.fit()


# TODO(ml-team): Test all the driver hooks once driver error propagation is fixed


@pytest.mark.parametrize("error_on", ["on_trial_result"])
def test_driver_error_with_tuner(ray_start_4_cpus, error_on):
    tuner = Tuner(
        trainable=passing_fn,
        run_config=tune.RunConfig(
            name=f"test_driver_errors_with_tuner-error_on={error_on}",
            callbacks=[FailingCallback(error_on=error_on)],
        ),
    )

    # All driver errors should get propagated to the user in the same way
    with pytest.raises(TuneError) as exc_info:
        tuner.fit()

    # TODO(ml-team): Assert the cause error type once driver error propagation is fixed
    assert "_TestSpecificError" in str(exc_info.value)


@pytest.mark.parametrize("error_at_level", ["worker", "coordinator"])
def test_preemption_handling(
    cluster_setup,
    tmp_path,
    error_at_level: str,
):
    """Integration test for node preemption handling in Ray Train/Tune.
    Even though `max_failures=0`, preemption errors should still be retried."""
    cluster, nodes, node_ids = cluster_setup
    # node 1 = coordinator and worker, node 2 = worker
    coordinator_node, worker_node = nodes
    coordinator_node_id, worker_node_id = node_ids

    num_workers = 2
    tmp_path.joinpath("markers").mkdir()

    def train_fn(config):
        checkpoint = train.get_checkpoint()
        start_iter = 0
        if checkpoint:
            start_iter = load_dict_checkpoint(checkpoint)["iter"] + 1
            print(f"Restored at iter = {start_iter}")

        for iter in range(start_iter, 6):
            with create_dict_checkpoint({"iter": iter}) as checkpoint:
                ray.train.report({"iter": iter}, checkpoint=checkpoint)

            if iter == 2:
                # Write a "done marker" to tell the driver to simulate a preemption.
                tmp_path.joinpath(
                    "markers", str(ray.train.get_context().get_world_rank())
                ).touch()
                # Await execution.
                time.sleep(120)

    def launch_training():
        trainer = DataParallelTrainer(
            train_loop_per_worker=train_fn,
            scaling_config=train.ScalingConfig(
                num_workers=num_workers,
                trainer_resources={"coordinator": 1},
                resources_per_worker={"cpu": 1},  # worker2 and worker3
            ),
            run_config=train.RunConfig(
                storage_path=str(tmp_path),
                name="test_preemption_error",
                failure_config=train.FailureConfig(fail_fast=False, max_failures=0),
            ),
        )
        result = trainer.fit()
        assert result.metrics["iter"] == 5

    t = threading.Thread(target=launch_training)
    t.start()

    # Wait until the workers are ready for preemption (after a few checkpoints).
    while len(list(tmp_path.joinpath("markers").glob("*"))) < num_workers:
        time.sleep(0.5)

    if error_at_level == "coordinator":
        node, node_id = coordinator_node, coordinator_node_id
    elif error_at_level == "worker":
        node, node_id = worker_node, worker_node_id
    else:
        raise NotImplementedError(f"Invalid error_at_level = {error_at_level}")

    # Preempt a node.
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    print("Draining node...")
    is_accepted, _ = gcs_client.drain_node(
        node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        0,
    )
    assert is_accepted
    print("Killing node...")
    cluster.remove_node(node, allow_graceful=True)
    print("Adding new node..")  # so that the job can be rescheduled
    # New node can replace a preempted coordinator or worker
    # NOTE: `cluster.add_node` only works in the main thread.
    cluster.add_node(resources={"coordinator": 1, "cpu": 1})
    t.join()  # Assert no errors during training.


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
