import pytest

import ray
from ray.train import RunConfig, ScalingConfig
from ray.train.tests.util import create_dict_checkpoint
from ray.train.v2._internal.constants import is_v2_enabled
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor
from ray.train.v2.api.config import FailureConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.tests.util import Counter, MockReplicaGroupBackendConfig

assert is_v2_enabled()


@pytest.fixture(scope="module", autouse=True)
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def test_report_with_worker_failure_before_sync(tmp_path):
    """Test that ray.train.report does not hang when a worker dies before
    reaching the synchronization barrier.

    Worker 1 raises ValueError on its first invocation (before calling report).
    Worker 0's first report is skipped (sync actor reset), then both workers
    successfully report on the second round.
    """
    counter = Counter.remote()

    def train_fn():
        rank = ray.train.get_context().get_world_rank()
        if rank == 0:
            with create_dict_checkpoint({"iter": 1}) as checkpoint:
                ray.train.report(metrics={"iter": 1}, checkpoint=checkpoint)
            with create_dict_checkpoint({"iter": 2}) as checkpoint:
                ray.train.report(metrics={"iter": 2}, checkpoint=checkpoint)
        else:
            count = ray.get(counter.increment.remote())
            if count == 1:
                raise ValueError("simulated worker failure")
            with create_dict_checkpoint({"iter": count}) as checkpoint:
                ray.train.report(metrics={"iter": count}, checkpoint=checkpoint)

    trainer = DataParallelTrainer(
        train_fn,
        backend_config=MockReplicaGroupBackendConfig(),
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            failure_config=FailureConfig(max_failures=1),
        ),
    )
    result = trainer.fit()
    assert result.error is None
    assert result.metrics["iter"] == 2
    assert len(result.best_checkpoints) == 1


def test_report_with_worker_failure_before_put_on_worker_queue(tmp_path):
    """Test that ray.train.report handles a worker dying after the sync barrier
    but before putting the result on the worker queue.

    Worker 1's checkpoint_upload_fn raises on its first invocation, causing the
    worker to die after passing the sync barrier. Worker 0 then syncs with replacement
    Worker 1 on the second report.

    This test simulates distributed training by using a barrier before each report.
    This avoids the race condition where we skip 1-2 reports e.g. Worker 0's first
    report is on the queue and Worker 0's second report is stuck in the sync barrier.
    In practice the user should never call ray.train.report twice without barriers
    in between.
    """
    counter = Counter.remote()
    sync_actor = SynchronizationActor.remote()

    def barrier():
        ray.get(
            sync_actor.broadcast_from_rank_zero.remote(
                world_rank=ray.train.get_context().get_world_rank(),
                world_size=ray.train.get_context().get_world_size(),
                data=None,
                caller_method_name="ray.train.collective.barrier",
            )
        )

    def train_fn():
        rank = ray.train.get_context().get_world_rank()
        if rank == 0:
            barrier()
            with create_dict_checkpoint({"iter": 1}) as checkpoint:
                ray.train.report(metrics={"iter": 1}, checkpoint=checkpoint)
            barrier()
            with create_dict_checkpoint({"iter": 2}) as checkpoint:
                ray.train.report(metrics={"iter": 2}, checkpoint=checkpoint)
        else:
            count = ray.get(counter.increment.remote())
            if count == 1:
                # First invocation: checkpoint + failing upload_fn to trigger
                # a failure after the sync barrier, during upload.
                def upload_fn(checkpoint, checkpoint_dir_name):
                    raise ValueError("simulated upload failure")

                with create_dict_checkpoint({"iter": count}) as checkpoint:
                    barrier()
                    ray.train.report(
                        metrics={"iter": count},
                        checkpoint=checkpoint,
                        checkpoint_upload_fn=upload_fn,
                    )
            else:
                # Second invocation: report without checkpoint (rank 0 handles it).
                barrier()
                ray.train.report(metrics={"iter": count}, checkpoint=None)

    trainer = DataParallelTrainer(
        train_fn,
        backend_config=MockReplicaGroupBackendConfig(),
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            failure_config=FailureConfig(max_failures=1),
        ),
    )
    result = trainer.fit()
    assert result.error is None
    assert result.metrics["iter"] == 2
    assert len(result.best_checkpoints) == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
