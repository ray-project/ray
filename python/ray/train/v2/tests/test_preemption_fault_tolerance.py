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
        # another collective on it is safe.
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


def _run_relax_trainer(tmp_path, preempted_rank, relax):
    """Run one attempt where `preempted_rank` is reclaimed before it reports.

    Faithful to the watcher: the same ``PreemptionInfo`` is set on every worker.
    The preempted rank then stalls without ever calling ``report()``, leaving
    the survivor alone at the barriers a ``report()`` crosses -- the
    ``get_preemption_info`` broadcast, then the checkpoint-dir-name broadcast
    and the consolidation gate. The step the run resumes from says whether the
    survivor's just-in-time checkpoint committed (step 1) or was lost and the
    run fell back to the earlier one (step 0).

    Defined inline so cloudpickle sends the training function by value; a
    module-level one is sent by reference and the worker cannot import it.
    """

    def train_fn(config):
        import time

        import ray.train
        from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
        from ray.train.v2._internal.execution.context import get_train_context
        from ray.train.v2.api.preemption import PreemptionInfo

        ckpt = ray.train.get_checkpoint()
        if ckpt is not None:
            # Resumed: report which checkpoint we came back from, then finish.
            # Reported with a checkpoint because `Result.metrics` carries the
            # metrics of the latest *checkpointed* report.
            resumed_from = load_dict_checkpoint(ckpt)["step"]
            with create_dict_checkpoint({"step": resumed_from}) as checkpoint:
                ray.train.report({"resumed_from": resumed_from}, checkpoint=checkpoint)
            return

        rank = ray.train.get_context().get_world_rank()
        preempted_rank = config["preempted_rank"]

        # A committed checkpoint to fall back to, standing in for the last
        # periodic one.
        with create_dict_checkpoint({"step": 0}) as checkpoint:
            ray.train.report({"step": 0}, checkpoint=checkpoint)

        # Long enough for the controller to notice the preemption (health
        # checks are 2s apart) and for the survivor to commit, short enough to
        # keep the test quick.
        get_train_context().preemption_context.preemption_info = PreemptionInfo(
            deadline_ms=int((time.time() + 12) * 1000),
            preempted_node_to_ranks={"mock-node": [preempted_rank]},
        )

        if rank == preempted_rank:
            # Reclaimed here, before reaching `report()`. The controller tears
            # this attempt down once the deadline elapses.
            time.sleep(60)
            return

        # Survivor. Both of these block on the preempted rank unless the
        # barrier has been relaxed.
        assert ray.train.get_preemption_info() is not None
        with create_dict_checkpoint({"step": 1}) as checkpoint:
            ray.train.report({"step": 1}, checkpoint=checkpoint)
        time.sleep(60)

    trainer = DataParallelTrainer(
        train_fn,
        train_loop_config={"preempted_rank": preempted_rank},
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            failure_config=FailureConfig(
                max_failures=0,
                max_preemption_failures=2,
                relax_collectives_on_preemption=relax,
            ),
        ),
    )
    return trainer.fit()


def test_report_barrier_strands_survivor_without_relaxation(tmp_path):
    """Without relaxation the survivor's just-in-time checkpoint is lost.

    The survivor blocks at the barrier waiting on a rank that will never
    arrive, so the run falls back to the last checkpoint that did commit.
    """
    result = _run_relax_trainer(tmp_path, preempted_rank=1, relax=False)

    assert result.error is None
    assert result.metrics["resumed_from"] == 0


def test_relaxed_report_barrier_commits_jit_checkpoint(tmp_path):
    """With relaxation the survivor commits without the preempted rank.

    ``max_failures=0`` also holds the line that this is still classified as a
    preemption and charged to ``max_preemption_failures``.
    """
    result = _run_relax_trainer(tmp_path, preempted_rank=1, relax=True)

    assert result.error is None
    assert result.metrics["resumed_from"] == 1


def test_report_barrier_not_relaxed_when_rank_0_is_preempted(tmp_path):
    """Relaxation is skipped when rank 0 is preempted, even when enabled.

    Rank 0 is the sole writer of the broadcast payload and the source of the
    consolidated report's metrics, so releasing without it would hand every
    survivor ``None``. Such runs keep the pre-relaxation behavior.
    """
    result = _run_relax_trainer(tmp_path, preempted_rank=0, relax=True)

    assert result.error is None
    assert result.metrics["resumed_from"] == 0


def _run_slow_checkpoint_trainer(tmp_path, grace_s):
    """A survivor whose checkpoint outlasts the drain window.

    The preempted rank stalls without reporting, and the survivor only reaches
    `report()` well after the deadline has passed. Committing then needs both
    halves of the feature: `relax_collectives_on_preemption` so the barrier
    completes without the preempted rank, and `preemption_grace_s`
    so the controller has not already torn the survivor down.
    """

    def train_fn(config):
        import json
        import os
        import tempfile
        import time

        import ray.train
        from ray.train import Checkpoint
        from ray.train.v2._internal.execution.context import get_train_context
        from ray.train.v2.api.preemption import PreemptionInfo

        ckpt = ray.train.get_checkpoint()
        if ckpt is not None:
            with ckpt.as_directory() as d:
                step = json.load(open(os.path.join(d, "s.json")))["step"]
            with tempfile.TemporaryDirectory() as t:
                json.dump({"step": step}, open(os.path.join(t, "s.json"), "w"))
                ray.train.report(
                    {"resumed_from": step},
                    checkpoint=Checkpoint.from_directory(t),
                )
            return

        rank = ray.train.get_context().get_world_rank()
        # A deadline only 5s out, far shorter than the checkpoint below.
        get_train_context().preemption_context.preemption_info = PreemptionInfo(
            deadline_ms=int((time.time() + 5) * 1000),
            preempted_node_to_ranks={"mock-node": [1]},
        )

        if rank == 1:
            time.sleep(120)  # reclaimed; never reports
            return

        # Survivor: a checkpoint that takes far longer than the drain window.
        time.sleep(15)
        with tempfile.TemporaryDirectory() as t:
            json.dump({"step": 42}, open(os.path.join(t, "s.json"), "w"))
            ray.train.report({"step": 42}, checkpoint=Checkpoint.from_directory(t))
        time.sleep(120)

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            failure_config=FailureConfig(
                max_failures=0,
                max_preemption_failures=2,
                relax_collectives_on_preemption=True,
                preemption_grace_s=grace_s,
            ),
        ),
    )
    return trainer.fit()


def test_survivor_finishes_checkpoint_past_the_deadline(tmp_path):
    """With grace, a checkpoint that outlasts the drain window still commits."""
    result = _run_slow_checkpoint_trainer(tmp_path, grace_s=60.0)

    assert result.error is None
    assert result.metrics["resumed_from"] == 42


def test_survivor_is_torn_down_at_the_deadline_without_grace(tmp_path):
    """Without grace the same checkpoint is lost -- the default behavior.

    The survivor is torn down mid-checkpoint, so nothing ever commits: every
    restart repeats the same preemption from scratch until the preemption
    budget runs out.
    """
    with pytest.raises(PreemptionError):
        _run_slow_checkpoint_trainer(tmp_path, grace_s=0.0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
