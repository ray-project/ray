import queue
import time
from unittest.mock import create_autospec

import pytest

from ray.actor import ActorHandle
from ray.train.v2._internal.constants import ENABLE_WORKER_STRUCTURED_LOGGING_ENV_VAR
from ray.train.v2._internal.execution.context import (
    DistributedContext,
    TrainRunContext,
    get_train_context,
)
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.worker_group.worker import RayTrainWorker
from ray.train.v2._internal.util import ObjectRefWrapper


@pytest.mark.parametrize("created_nested_threads", [True, False])
def test_worker_finished_after_all_threads_finish(monkeypatch, created_nested_threads):
    # Disable this to avoid TypeError from logging MagicMock
    monkeypatch.setenv(ENABLE_WORKER_STRUCTURED_LOGGING_ENV_VAR, False)

    # Initialize RayTrainWorker state
    worker = RayTrainWorker()
    worker.init_train_context(
        train_run_context=create_autospec(TrainRunContext, instance=True),
        distributed_context=DistributedContext(
            world_rank=0,
            world_size=1,
            local_rank=0,
            local_world_size=1,
            node_rank=0,
        ),
        synchronization_actor=create_autospec(ActorHandle, instance=True),
        storage_context=create_autospec(StorageContext, instance=True),
        worker_callbacks=[],
        controller_actor=create_autospec(ActorHandle, instance=True),
    )
    global_queue = queue.Queue()

    def train_fn():
        tc = get_train_context()

        def target():
            # Intentionally sleep longer than poll interval to test that we wait
            # for nested threads to finish
            time.sleep(0.1)
            global_queue.put("nested")

        if created_nested_threads:
            tc.checkpoint_upload_threadpool.submit(target)
        else:
            global_queue.put("main")

    # Run train fn and wait for it to finish
    train_fn_ref = create_autospec(ObjectRefWrapper, instance=True)
    train_fn_ref.get.return_value = train_fn
    worker.run_train_fn(train_fn_ref)
    while worker.poll_status().running:
        time.sleep(0.01)

    # Verify queue contents
    queue_contents = []
    while not global_queue.empty():
        queue_contents.append(global_queue.get())
    if created_nested_threads:
        assert queue_contents == ["nested"]
    else:
        assert queue_contents == ["main"]


def test_training_thread_name_matches_user_fn(monkeypatch):
    """The training thread should be named after the user's training function.

    `run_train_fn` wraps the user function in an internal
    `train_fn_with_final_checkpoint_flush` closure before handing it to the
    `ThreadRunner`, which names the thread after the callable it runs. Without
    `functools.wraps`, the thread (and therefore any stack trace produced from
    the user function) would be labeled with the internal wrapper name instead
    of the user function name, making errors harder to trace.
    """
    # Disable this to avoid TypeError from logging MagicMock
    monkeypatch.setenv(ENABLE_WORKER_STRUCTURED_LOGGING_ENV_VAR, False)

    worker = RayTrainWorker()
    worker.init_train_context(
        train_run_context=create_autospec(TrainRunContext, instance=True),
        distributed_context=DistributedContext(
            world_rank=0,
            world_size=1,
            local_rank=0,
            local_world_size=1,
            node_rank=0,
        ),
        synchronization_actor=create_autospec(ActorHandle, instance=True),
        storage_context=create_autospec(StorageContext, instance=True),
        worker_callbacks=[],
        controller_actor=create_autospec(ActorHandle, instance=True),
    )

    def my_custom_train_fn():
        pass

    train_fn_ref = create_autospec(ObjectRefWrapper, instance=True)
    train_fn_ref.get.return_value = my_custom_train_fn
    worker.run_train_fn(train_fn_ref)

    # The training thread is created synchronously inside `run_train_fn`, so its
    # name is already set by the time the call returns -- there is no need to
    # wait for the training function to run or finish.
    training_thread = (
        get_train_context().execution_context.training_thread_runner._thread
    )
    assert training_thread.name == "TrainingThread(my_custom_train_fn)"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
