import asyncio
import os
import sys
import time
from collections import defaultdict

import pytest

import ray
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.exceptions import TaskCancelledError
from ray.util.state import list_tasks


def test_input_validation(shutdown_only):
    # Verify force=True is not working.
    @ray.remote
    class A:
        async def f(self):
            pass

    a = A.remote()
    with pytest.raises(TypeError, match="force=True is not supported"):
        ray.cancel(a.f.remote(), force=True)


def test_async_actor_cancel(shutdown_only):
    """
    Test async actor task is canceled and
    asyncio.CancelledError is raised within a task.

    If a task is canceled while it is executed,
    it should raise RayTaskError.

    TODO(sang): It is awkward we raise RayTaskError
    when a task is interrupted. Should we just raise
    TaskCancelledError? It is an API change.
    """
    ray.init(num_cpus=1)

    @ray.remote
    class VerifyActor:
        def __init__(self):
            self.called = False

        def called(self):
            print("called")
            self.called = True

        def is_called(self):
            print("is caled, ", self.called)
            return self.called

    @ray.remote
    class Actor:
        async def f(self, verify_actor):
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                ray.get(verify_actor.called.remote())
                assert asyncio.get_current_task.canceled()
                return True
            return False

    v = VerifyActor.remote()
    a = Actor.remote()
    ref = a.f.remote(v)
    ray.get(a.__ray_ready__.remote())
    ray.get(v.__ray_ready__.remote())
    ray.cancel(ref)

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(ref)

    # Verify asyncio.CancelledError is raised from the actor task.
    assert ray.get(v.is_called.remote())


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


@pytest.mark.skip(
    reason=("The guarantee in this case is too weak now. " "Need more work.")
)
def test_in_flight_queued_requests_canceled(shutdown_only, monkeypatch):
    """
    When there are large input size in-flight actor tasks
    tasks are queued inside a RPC layer (core_worker_client.h)
    In this case, we don't cancel a request from a client side
    but wait until it is sent to the server side and cancel it.
    See SendRequests() inside core_worker_client.h
    """
    # Currently the max bytes is
    # const int64_t kMaxBytesInFlight = 16 * 1024 * 1024.
    # See core_worker_client.h.
    input_arg = b"1" * 15 * 1024  # 15KB.
    # Tasks are queued when there are more than 1024 tasks.
    sig = SignalActor.remote()

    @ray.remote
    class Actor:
        def __init__(self, signal_actor):
            self.signal_actor = signal_actor

        def f(self, input_arg):
            ray.get(self.signal_actor.wait.remote())
            return True

    a = Actor.remote(sig)
    refs = [a.f.remote(input_arg) for _ in range(5000)]

    # Wait until the first task runs.
    wait_for_condition(
        lambda: len(list_tasks(filters=[("STATE", "=", "RUNNING")])) == 1
    )

    # Cancel all tasks.
    for ref in refs:
        ray.cancel(ref)

    # The first ref is in progress, so we pop it out
    first_ref = refs.pop(0)
    ray.get(sig.send.remote())

    # Make sure all tasks that are queued (including queued
    # due to in-flight bytes) are canceled.
    canceled = 0
    for ref in refs:
        try:
            ray.get(ref)
        except TaskCancelledError:
            canceled += 1

    # Verify at least half of tasks are canceled.
    # Currently, the guarantee is weak because we cannot
    # detect queued tasks due to inflight bytes limit.
    # TODO(sang): Move the in flight bytes logic into
    # actor submission queue instead of doing it inside
    # core worker client.
    assert canceled > 2500

    # first ref shouldn't have been canceled.
    assert ray.get(first_ref)


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
                    ("STATE", "=", "SUBMITTED_TO_WORKER"),
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
    # assert task.state == "SUBMITTED_TO_WORKER"
    for task in tasks:
        assert task.state == "SUBMITTED_TO_WORKER"


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
        # Since an actor is dead, in this case, it will raise
        # RayActorError.
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(ref)

        # This will restart actor, but task won't be retried.
        cluster.add_node(num_cpus=1)
        # Verify actor is restarted. f should be retried
        ray.get(a.__ray_ready__.remote())
        with pytest.raises(ray.exceptions.RayActorError):
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

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(sleep_ref)


@pytest.mark.skip(reason=("Currently not passing. There's one edge case to fix."))
def test_cancel_stress(shutdown_only):
    ray.init()

    @ray.remote
    class Actor:
        async def sleep(self):
            await asyncio.sleep(1000)

    actors = [Actor.remote() for _ in range(30)]

    refs = []
    for _ in range(20):
        for actor in actors:
            for i in range(100):
                ref = actor.sleep.remote()
                refs.append(ref)
                if i % 2 == 0:
                    ray.cancel(ref)

    for ref in refs:
        ray.cancel(ref)

    for ref in refs:
        with pytest.raises((ray.exceptions.RayTaskError, TaskCancelledError)):
            ray.get(ref)


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
        with pytest.raises(ray.exceptions.RayTaskError):
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

    with pytest.raises(ray.exceptions.RayTaskError):
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
                with pytest.raises(ray.exceptions.RayTaskError):
                    ray.get(ref)
        else:
            for ref in children_refs:
                assert ray.get(ref)

        with pytest.raises(ray.exceptions.RayTaskError):
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
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(ref)
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(ray.get(r4.get_child_ref.remote()))
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(ray.get(r3.get_child_ref.remote()))
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(ray.get(r2.get_child_ref.remote()))
    else:
        assert ray.get(ray.get(r2.get_child_ref.remote()))
        assert ray.get(ray.get(r3.get_child_ref.remote()))
        assert ray.get(ray.get(r4.get_child_ref.remote()))

        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(ref)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
