import asyncio
import concurrent.futures
import sys
import time
from collections import defaultdict

import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.exceptions import TaskCancelledError
from ray.util.state import list_tasks


def test_input_validation(shutdown_only):
    # Verify force=True is not working.
    @ray.remote
    class A:
        async def f(self):
            pass

    a = A.remote()
    with pytest.raises(ValueError, match="force=True is not supported"):
        ray.cancel(a.f.remote(), force=True)


def test_async_actor_cancel(shutdown_only):
    """
    Test async actor task is canceled and
    asyncio.CancelledError is raised within a task.
    """
    ray.init(num_cpus=1)

    @ray.remote
    class VerifyActor:
        def __init__(self):
            self.called = False
            self.running = False

        def called(self):
            self.called = True

        def set_running(self):
            self.running = True

        def is_called(self):
            return self.called

        def is_running(self):
            return self.running

        def reset(self):
            self.called = False
            self.running = False

    @ray.remote
    class Actor:
        async def f(self, verify_actor):
            try:
                ray.get(verify_actor.set_running.remote())
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                # It is False until this except block is finished.
                print(asyncio.current_task().cancelled())
                assert not asyncio.current_task().cancelled()
                ray.get(verify_actor.called.remote())
                raise
            except Exception:
                return True
            return True

    v = VerifyActor.remote()
    a = Actor.remote()

    for i in range(50):
        ref = a.f.remote(v)
        wait_for_condition(lambda: ray.get(v.is_running.remote()))
        ray.cancel(ref)

        with pytest.raises(ray.exceptions.TaskCancelledError, match="was cancelled"):
            ray.get(ref)

        # Verify asyncio.CancelledError is raised from the actor task.
        assert ray.get(v.is_running.remote())
        assert ray.get(v.is_called.remote())
        ray.get(v.reset.remote())


def test_async_actor_client_side_cancel(ray_start_cluster):
    """
    Test a task is cancelled while it is queued on a client side.
    It should raise ray.exceptions.TaskCancelledError.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            self.f_called = False

        async def g(self, ref):
            await asyncio.sleep(30)

        async def f(self):
            self.f_called = True
            await asyncio.sleep(5)

        def is_f_called(self):
            return self.f_called

    @ray.remote
    def f():
        time.sleep(100)

    # Test the case where a task is queued on a client side.
    # Tasks are not sent until actor is created.
    a = Actor.remote()
    ref = a.f.remote()
    ray.cancel(ref)
    with pytest.raises(TaskCancelledError):
        ray.get(ref)

    cluster.add_node(num_cpus=1)
    assert not ray.get(a.is_f_called.remote())

    # Test the case where it is canceled before dependencies
    # are resolved.
    a = Actor.remote()
    ref_dep_not_resolved = a.g.remote(f.remote())
    ray.cancel(ref_dep_not_resolved)
    with pytest.raises(TaskCancelledError):
        ray.get(ref_dep_not_resolved)


def test_async_actor_server_side_cancel(shutdown_only):
    """
    Test Cancelation when a task is queued on a server side.
    """

    @ray.remote
    class Actor:
        async def f(self):
            await asyncio.sleep(5)

        async def g(self):
            await asyncio.sleep(0)

    a = Actor.options(max_concurrency=1).remote()
    ray.get(a.__ray_ready__.remote())
    ref = a.f.remote()  # noqa
    # Queued on a server side.
    # Task should not be executed at all.
    refs = [a.g.remote() for _ in range(100)]
    wait_for_condition(
        lambda: len(
            list_tasks(
                filters=[
                    ("name", "=", "Actor.g"),
                    ("STATE", "=", "PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY"),
                ]
            )
        )
        == 100
    )

    for ref in refs:
        ray.cancel(ref)
    tasks = list_tasks(filters=[("name", "=", "Actor.g")])

    for ref in refs:
        with pytest.raises(TaskCancelledError, match=ref.task_id().hex()):
            ray.get(ref)

    # Verify the task is submitted to the worker and never executed
    for task in tasks:
        assert task.state == "PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY"


def test_async_actor_cancel_after_task_finishes(shutdown_only):
    @ray.remote
    class Actor:
        async def f(self):
            await asyncio.sleep(5)

        async def empty(self):
            pass

    # Cancel after task finishes
    a = Actor.options(max_concurrency=1).remote()
    ref = a.empty.remote()
    ref2 = a.empty.remote()
    ray.get([ref, ref2])
    ray.cancel(ref)
    ray.cancel(ref2)
    # Exceptions shouldn't be raised.
    ray.get([ref, ref2])


def test_async_actor_cancel_restart(ray_start_cluster, monkeypatch):
    """
    Verify a cancelation works if actor is restarted.
    """
    with monkeypatch.context() as m:
        # This will slow down the cancelation RPC so that
        # cancel won't succeed until a node is killed.
        m.setenv(
            "RAY_testing_asio_delay_us",
            "CoreWorkerService.grpc_server.CancelTask=3000000:3000000",
        )
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=0)
        ray.init(address=cluster.address)
        node = cluster.add_node(num_cpus=1)

        @ray.remote(num_cpus=1, max_restarts=-1, max_task_retries=-1)
        class Actor:
            async def f(self):
                await asyncio.sleep(10)

        a = Actor.remote()
        ref = a.f.remote()
        # This guarantees that a.f.remote() is executed
        ray.get(a.__ray_ready__.remote())
        ray.cancel(ref)
        cluster.remove_node(node)
        r, ur = ray.wait([ref])
        # When cancel is called, the task won't be retried anymore.
        # It will raise TaskCancelledError.
        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(ref)

        # This will restart actor, but task won't be retried.
        cluster.add_node(num_cpus=1)
        # Verify actor is restarted. f should be retried
        ray.get(a.__ray_ready__.remote())
        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(ref)


def test_remote_cancel(ray_start_regular):
    @ray.remote
    class Actor:
        async def sleep(self):
            await asyncio.sleep(1000)

    @ray.remote
    def f(refs):
        ref = refs[0]
        ray.cancel(ref)

    a = Actor.remote()
    sleep_ref = a.sleep.remote()
    wait_for_condition(lambda: list_tasks(filters=[("name", "=", "Actor.sleep")]))
    ref = f.remote([sleep_ref])  # noqa

    with pytest.raises(ray.exceptions.TaskCancelledError):
        ray.get(sleep_ref)


def test_cancel_recursive_tree(shutdown_only):
    """Verify recursive cancel works for tree-nested tasks.

    Task A -> Task B
           -> Task C
    """
    ray.init(num_cpus=16)

    # Test the tree structure.
    @ray.remote
    def child():
        for _ in range(5):
            time.sleep(1)
        return True

    @ray.remote
    class ChildActor:
        async def child(self):
            await asyncio.sleep(5)
            return True

    @ray.remote
    class Actor:
        def __init__(self):
            self.children_refs = defaultdict(list)

        def get_children_refs(self, task_id):
            return self.children_refs[task_id]

        async def run(self, child_actor, sig):
            ref1 = child.remote()
            ref2 = child_actor.child.remote()
            task_id = ray.get_runtime_context().get_task_id()
            self.children_refs[task_id].append(ref1)
            self.children_refs[task_id].append(ref2)

            await sig.wait.remote()
            await ref1
            await ref2

    sig = SignalActor.remote()
    child_actor = ChildActor.remote()
    a = Actor.remote()
    ray.get(a.__ray_ready__.remote())

    """
    Test the basic case.
    """
    run_ref = a.run.remote(child_actor, sig)
    task_id = run_ref.task_id().hex()
    wait_for_condition(
        lambda: list_tasks(filters=[("task_id", "=", task_id)])[0].state == "RUNNING"
    )
    ray.cancel(run_ref, recursive=True)
    ray.get(sig.send.remote())

    children_refs = ray.get(a.get_children_refs.remote(task_id))

    for ref in children_refs + [run_ref]:
        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(ref)

    """
    Test recursive = False
    """
    run_ref = a.run.remote(child_actor, sig)
    task_id = run_ref.task_id().hex()
    wait_for_condition(
        lambda: list_tasks(filters=[("task_id", "=", task_id)])[0].state == "RUNNING"
    )
    ray.cancel(run_ref, recursive=False)
    ray.get(sig.send.remote())

    children_refs = ray.get(a.get_children_refs.remote(task_id))

    for ref in children_refs:
        assert ray.get(ref)

    with pytest.raises(ray.exceptions.TaskCancelledError):
        ray.get(run_ref)

    """
    Test concurrent cases.
    """
    run_refs = [a.run.remote(ChildActor.remote(), sig) for _ in range(10)]
    task_ids = []

    for i, run_ref in enumerate(run_refs):
        task_id = run_ref.task_id().hex()
        task_ids.append(task_id)
        wait_for_condition(
            lambda task_id=task_id: list_tasks(filters=[("task_id", "=", task_id)])[
                0
            ].state
            == "RUNNING"
        )
        children_refs = ray.get(a.get_children_refs.remote(task_id))
        for child_ref in children_refs:
            task_id = child_ref.task_id().hex()
            wait_for_condition(
                lambda task_id=task_id: list_tasks(filters=[("task_id", "=", task_id)])[
                    0
                ].state
                == "RUNNING"
            )
        recursive = i % 2 == 0
        ray.cancel(run_ref, recursive=recursive)

    ray.get(sig.send.remote())

    for i, task_id in enumerate(task_ids):
        children_refs = ray.get(a.get_children_refs.remote(task_id))

        if i % 2 == 0:
            for ref in children_refs:
                with pytest.raises(ray.exceptions.TaskCancelledError):
                    ray.get(ref)
        else:
            for ref in children_refs:
                assert ray.get(ref)

        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(run_ref)


@pytest.mark.parametrize("recursive", [True, False])
def test_cancel_recursive_chain(shutdown_only, recursive):
    @ray.remote
    class RecursiveActor:
        def __init__(self, child=None):
            self.child = child
            self.chlid_ref = None

        async def run(self, sig):
            if self.child is None:
                await sig.wait.remote()
                return True

            ref = self.child.run.remote(sig)
            self.child_ref = ref
            return await ref

        def get_child_ref(self):
            return self.child_ref

    sig = SignalActor.remote()
    r1 = RecursiveActor.remote()
    r2 = RecursiveActor.remote(r1)
    r3 = RecursiveActor.remote(r2)
    r4 = RecursiveActor.remote(r3)

    ref = r4.run.remote(sig)
    ray.get(r4.__ray_ready__.remote())
    wait_for_condition(
        lambda: len(list_tasks(filters=[("name", "=", "RecursiveActor.run")])) == 4
    )
    ray.cancel(ref, recursive=recursive)
    ray.get(sig.send.remote())

    if recursive:
        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(ref)
        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(ray.get(r4.get_child_ref.remote()))
        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(ray.get(r3.get_child_ref.remote()))
        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(ray.get(r2.get_child_ref.remote()))
    else:
        assert ray.get(ray.get(r2.get_child_ref.remote()))
        assert ray.get(ray.get(r3.get_child_ref.remote()))
        assert ray.get(ray.get(r4.get_child_ref.remote()))

        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(ref)


def test_concurrent_submission_and_cancellation(shutdown_only):
    """Test submitting and then cancelling many tasks concurrently.

    This is a regression test for race conditions such as:
        https://github.com/ray-project/ray/issues/52628.
    """
    NUM_TASKS = 2500

    @ray.remote(num_cpus=0)
    class Worker:
        async def sleep(self, i: int):
            # NOTE: all tasks should be cancelled, so this won't actually sleep for the
            # full duration if the test is passing.
            await asyncio.sleep(30)

    worker = Worker.remote()

    # Submit many tasks in parallel to cause queueing on the caller and receiver.
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_TASKS) as executor:
        futures = [executor.submit(worker.sleep.remote, i) for i in range(NUM_TASKS)]
        refs = [f.result() for f in concurrent.futures.as_completed(futures)]

    # Cancel the tasks in reverse order of submission.
    for ref in reversed(refs):
        ray.cancel(ref)

    # Check that all tasks were successfully cancelled (none ran to completion).
    for ref in refs:
        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(ref)

    print(f"All {NUM_TASKS} tasks were cancelled successfully.")


def test_is_canceled_sync_actor_task(shutdown_only):
    """Test that is_canceled() works correctly for sync actor tasks."""

    @ray.remote
    class Actor:
        def __init__(self):
            self.is_canceled = False

        def task_with_cancel_check(self, signal_actor):
            ray.get(signal_actor.wait.remote())

            # Check if the task was cancelled
            if ray.get_runtime_context().is_canceled():
                self.is_canceled = True
                return "canceled"

            return "completed"

        def is_canceled(self):
            return self.is_canceled

    sig = SignalActor.remote()
    actor = Actor.remote()

    ref = actor.task_with_cancel_check.remote(sig)

    # Wait for the task to be actively waiting on the signal
    wait_for_condition(lambda: ray.get(sig.cur_num_waiters.remote()) == 1)

    # Cancel the task while it's blocked on the signal
    ray.cancel(ref, recursive=False)
    ray.get(sig.send.remote())

    # The task should raise TaskCancelledError
    with pytest.raises(TaskCancelledError):
        ray.get(ref)

    # Verify the actor state is changed
    assert ray.get(actor.is_canceled.remote())


def test_is_canceled_concurrent_actor_task(shutdown_only):
    """Test that is_canceled() works correctly for concurrent actor tasks."""

    @ray.remote
    class ConcurrentActor:
        def __init__(self):
            self.canceled_tasks = set()

        def task_with_cancel_check(self, task_id, signal_actor):
            ray.get(signal_actor.wait.remote())

            # Check if the task was cancelled
            if ray.get_runtime_context().is_canceled():
                self.canceled_tasks.add(task_id)
                return f"task_{task_id}_canceled"

            return f"task_{task_id}_completed"

        def get_canceled_tasks(self):
            return self.canceled_tasks

    sig = SignalActor.remote()
    actor = ConcurrentActor.options(max_concurrency=3).remote()

    # Submit multiple tasks concurrently
    refs = [actor.task_with_cancel_check.remote(i, sig) for i in range(3)]

    # Wait for all tasks to be waiting on the signal
    wait_for_condition(lambda: ray.get(sig.cur_num_waiters.remote()) == 3)

    # Cancel one of the task
    ray.cancel(refs[1], recursive=False)

    # Send signal to unblock all tasks
    ray.get(sig.send.remote())

    # Canceled tasks should raise TaskCancelledError
    with pytest.raises(TaskCancelledError):
        ray.get(refs[1])

    # Other tasks should complete normally
    for i in [0, 2]:
        result = ray.get(refs[i])
        assert result == f"task_{i}_completed"

    # Verify that task 1 is marked canceled
    canceled_tasks = ray.get(actor.get_canceled_tasks.remote())
    assert canceled_tasks == {1}


def test_is_canceled_not_supported_in_async_actor(shutdown_only):
    """Test is_canceled() for async actors."""

    @ray.remote
    class AsyncActor:
        def __init__(self):
            self.is_canceled = False

        async def async_task(self):
            # is_canceled() doesn't work for async actors
            if ray.get_runtime_context().is_canceled():
                self.is_canceled = True
                return "canceled"
            return "completed"

        def is_canceled(self):
            return self.is_canceled

    actor = AsyncActor.remote()
    ref = actor.async_task.remote()

    # is_canceled() is not supported for async actors
    with pytest.raises(
        RuntimeError, match="This method is not supported in an async actor."
    ):
        ray.get(ref)

    # Verify the state for async actor does NOT change as there's no graceful
    # termination for async actor task
    assert not ray.get(actor.is_canceled.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
