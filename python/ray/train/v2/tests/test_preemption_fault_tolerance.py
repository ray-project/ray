"""End-to-end preemption tests with a real ``DataParallelTrainer``.

Only the preemption *signal* is mocked: the training function sets its own
``PreemptionContext`` via ``get_train_context().preemption_context.set(...)``,
which is byte-for-byte what ``RayTrainWorker.mark_preempt`` does when the
``PreemptionWatcher`` detects a node drain. Everything else -- the worker group,
the controller state machine (RunningState -> PreemptingState), the failure
policy, checkpoint resume -- is real.

The controller restarts a run on an *actual* reclaim (a preempted worker death)
or when the reclaim deadline elapses; a run that returns cleanly finishes
regardless of any signal. A real preempted death can't be injected in-process,
so the restart path is exercised here via the deadline (state-machine coverage
of the death path lives in test_controller.py).
"""

import pytest

import ray
from ray.train import FailureConfig, RunConfig, ScalingConfig
from ray.train.v2._internal.constants import is_v2_enabled
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer

assert is_v2_enabled()


@pytest.fixture(scope="module", autouse=True)
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def test_preemption_clean_finish(tmp_path):
    """A run that returns cleanly finishes even with an active preemption signal.

    ``max_preemption_failures=0`` and ``max_failures=0``: the run only succeeds
    if the clean return is treated as a completion, not a preemption restart.
    """

    def train_fn():
        import ray.train
        from ray.train.tests.util import create_dict_checkpoint
        from ray.train.v2._internal.execution.context import get_train_context
        from ray.train.v2._internal.execution.preemption import PreemptionInfo

        # Signal present the whole time; the UDF reads it but keeps training and
        # returns normally.
        get_train_context().preemption_context.set(
            PreemptionInfo(
                deadline_ms=None,
                preempted_node_to_ranks={"mock-node": [0, 1]},
            )
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
            get_train_context().preemption_context.set(
                PreemptionInfo(
                    deadline_ms=1,  # epoch 1ms -> always in the past
                    preempted_node_to_ranks={"mock-node": [0, 1]},
                )
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
