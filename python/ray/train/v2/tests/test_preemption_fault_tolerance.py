"""End-to-end preemption tests with a real ``DataParallelTrainer``.

Only the preemption info is mocked: the training function sets its own
``PreemptionContext`` (byte-for-byte what ``RayTrainWorker.mark_preempt`` does
when the ``PreemptionWatcher`` detects a node drain). Everything else -- the
worker group, the controller state machine (RunningState -> PreemptingState),
the failure policy, checkpoint resume -- is real. This keeps these tests fast
and deterministic on a single node. Injecting the info directly (rather than
mocking Ray Core's drain state) is deliberate: the PreemptionWatcher polls
``get_draining_nodes`` inside its own actor process, so a driver-side mock
cannot reach it; exercising a real GCS drain requires a multi-node cluster.
"""

import pytest

import ray
from ray.train import (
    FailureConfig,
    PreemptionError,
    RunConfig,
    ScalingConfig,
    WorkerGroupError,
)
from ray.train.v2._internal.constants import is_v2_enabled
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer

assert is_v2_enabled()


@pytest.fixture(scope="module", autouse=True)
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def test_preemption_clean_finish(tmp_path):
    """A run that returns cleanly finishes even with an active preemption.

    ``max_preemption_failures=0`` and ``max_failures=0``: the run only succeeds
    if the clean return is treated as a completion, not a preemption restart.
    """

    def train_fn():
        import ray.train
        from ray.train.tests.util import create_dict_checkpoint
        from ray.train.v2._internal.execution.context import get_train_context
        from ray.train.v2._internal.execution.preemption import PreemptionInfo

        # Preemption in progress the whole time; the UDF reads it but keeps
        # training and returns normally.
        get_train_context().preemption_context.preemption_info = PreemptionInfo(
            deadline_ms=None,
            preempted_node_to_ranks={"mock-node": [0, 1]},
        )
        for step in range(2):
            assert ray.train.get_preemption_info() is not None
            with create_dict_checkpoint({"step": step}) as checkpoint:
                ray.train.report({"step": step}, checkpoint=checkpoint)

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            failure_config=FailureConfig(max_failures=0, max_preemption_failures=0),
        ),
    )
    result = trainer.fit()

    assert result.error is None
    assert result.metrics["step"] == 1


@pytest.mark.parametrize("notified_rank", [0, 1])
def test_preemption_info_is_consistent_across_ranks(tmp_path, notified_rank):
    """Every rank reads the same value, whichever rank received the notice.

    That consistency is what makes the return value safe to use as the condition
    for another collective. With a local read, a notice that had reached only one
    rank would make that rank enter `report()` while its peer ran another training
    step, and the two would deadlock against each other.

    The value is rank 0's view, so a notice that has only reached a nonzero rank
    reads as "no preemption" on every rank -- consistently, and only until the
    watcher's fan-out reaches rank 0 too.
    """

    def train_fn(config):
        import ray.train
        from ray.train.tests.util import create_dict_checkpoint
        from ray.train.v2._internal.execution.context import get_train_context
        from ray.train.v2.api.preemption import PreemptionInfo

        rank = ray.train.get_context().get_world_rank()
        if rank == config["notified_rank"]:
            get_train_context().preemption_context.preemption_info = PreemptionInfo(
                deadline_ms=1234,
                preempted_node_to_ranks={"mock-node": [config["notified_rank"]]},
            )

        info = ray.train.get_preemption_info()
        rank_zero_was_notified = config["notified_rank"] == 0
        assert (info is not None) == rank_zero_was_notified, (
            f"rank {rank} read {info!r}; every rank should agree on "
            f"{'the notice' if rank_zero_was_notified else 'None'}"
        )
        if info is not None:
            assert info.preempted_ranks == [0]
            assert info.preempted_node_ids == ["mock-node"]
            assert info.deadline_ms == 1234

        # Whatever the answer, it is the same on every rank, so branching into
        # another collective on it is safe. Report a checkpoint so the metrics
        # reach the driver -- Train V2 does not persist metrics on their own.
        metrics = {"observed_preemption": info is not None}
        with create_dict_checkpoint(metrics) as checkpoint:
            ray.train.report(metrics, checkpoint=checkpoint)

    trainer = DataParallelTrainer(
        train_fn,
        train_loop_config={"notified_rank": notified_rank},
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            failure_config=FailureConfig(max_failures=0, max_preemption_failures=0),
        ),
    )
    result = trainer.fit()

    assert result.error is None
    assert result.metrics["observed_preemption"] is (notified_rank == 0)


def test_preemption_info_falls_back_when_barrier_is_reset(tmp_path):
    """A barrier reset mid-call degrades to the local value instead of raising.

    `get_preemption_info()` is a collective, so it is exposed to the same barrier
    reset that `report()` already tolerates -- and it is called precisely when
    workers are dying and the controller resets the barrier. It must not surface
    an internal Ray error to the training function.
    """

    def train_fn():
        import time

        import ray
        import ray.train
        from ray.train.v2._internal.execution.context import get_train_context
        from ray.train.v2.api.preemption import PreemptionInfo

        rank = ray.train.get_context().get_world_rank()
        get_train_context().preemption_context.preemption_info = PreemptionInfo(
            deadline_ms=None, preempted_node_to_ranks={"mock-node": [rank]}
        )
        sync_actor = get_train_context().get_synchronization_actor()

        if rank == 1:
            # Stand in for a worker that dies before joining: wait until rank 0 is
            # inside the collective, then reset the barrier the way the controller
            # does on replica group replacement.
            deadline = time.time() + 30
            while ray.get(sync_actor.get_counter.remote()) < 1:
                assert time.time() < deadline, "rank 0 never joined the collective"
                time.sleep(0.05)
            ray.get(sync_actor.reset.remote())
            return

        # Rank 0 is the one caught in the reset.
        info = ray.train.get_preemption_info()
        assert info is not None, "expected a fallback to the local value"
        assert info.preempted_ranks == [0], info

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            failure_config=FailureConfig(max_failures=0, max_preemption_failures=0),
        ),
    )
    result = trainer.fit()
    assert result.error is None


def test_preemption_deadline_restart_and_resume(tmp_path):
    """When the reclaim deadline elapses while workers are still running, the
    controller tears them down and restarts, resuming from the last checkpoint.

    ``max_failures=0`` is the key assertion: if the interruption were treated as
    a generic worker error it would raise immediately. The run succeeding proves
    it was classified as a preemption and retried against
    ``max_preemption_failures``.
    """

    def train_fn():
        import time

        import ray.train
        from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
        from ray.train.v2._internal.execution.context import get_train_context
        from ray.train.v2._internal.execution.preemption import PreemptionInfo

        ckpt = ray.train.get_checkpoint()
        if ckpt is None:
            # First attempt: checkpoint, then signal a reclaim with an
            # already-passed deadline and keep running so the controller
            # force-tears-us-down at the deadline.
            with create_dict_checkpoint({"step": 0}) as checkpoint:
                ray.train.report({"step": 0}, checkpoint=checkpoint)
            get_train_context().preemption_context.preemption_info = PreemptionInfo(
                deadline_ms=1,  # epoch 1ms -> always in the past
                preempted_node_to_ranks={"mock-node": [0, 1]},
            )
            time.sleep(60)  # interrupted by the forced teardown well before this
        else:
            # Resumed attempt: finish.
            step = load_dict_checkpoint(ckpt)["step"] + 1
            with create_dict_checkpoint({"step": step}) as checkpoint:
                ray.train.report({"step": step}, checkpoint=checkpoint)

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            failure_config=FailureConfig(max_failures=0, max_preemption_failures=2),
        ),
    )
    result = trainer.fit()

    assert result.error is None
    # Resumed from the step-0 checkpoint and ran to completion.
    assert result.metrics["step"] == 1


def test_preemption_budget_exhausted(tmp_path):
    """When preemption restarts exceed ``max_preemption_failures``, the run
    raises a ``PreemptionError`` (not a generic worker error)."""

    def train_fn():
        import time

        from ray.train.v2._internal.execution.context import get_train_context
        from ray.train.v2._internal.execution.preemption import PreemptionInfo

        # Every attempt is preempted with an already-passed deadline and keeps
        # running, so the controller force-tears-down each attempt.
        get_train_context().preemption_context.preemption_info = PreemptionInfo(
            deadline_ms=1,  # epoch 1ms -> always in the past
            preempted_node_to_ranks={"mock-node": [0, 1]},
        )
        time.sleep(60)  # interrupted by the forced teardown well before this

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            failure_config=FailureConfig(max_failures=0, max_preemption_failures=1),
        ),
    )
    with pytest.raises(PreemptionError):
        trainer.fit()


def test_user_error_with_signal_is_worker_failure(tmp_path):
    """A training function bug while a preemption is in progress is still a
    worker failure charged to ``max_failures`` -- it is not masked by the
    (unlimited-by-default) preemption budget."""

    def train_fn():
        from ray.train.v2._internal.execution.context import get_train_context
        from ray.train.v2._internal.execution.preemption import PreemptionInfo

        get_train_context().preemption_context.preemption_info = PreemptionInfo(
            deadline_ms=None,
            preempted_node_to_ranks={"mock-node": [0, 1]},
        )
        raise RuntimeError("bug in jit checkpoint")

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            failure_config=FailureConfig(max_failures=0, max_preemption_failures=-1),
        ),
    )
    with pytest.raises(WorkerGroupError):
        trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
