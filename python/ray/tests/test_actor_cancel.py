import os
import random
import signal
import sys
import threading
import _thread
import time

import pytest

import ray
from ray.exceptions import (
    TaskCancelledError,
    RayTaskError,
    GetTimeoutError,
    WorkerCrashedError,
    ObjectLostError,
)
from ray._private.utils import DeferSigint
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.util.state import list_tasks

import ray
import asyncio
import time
import concurrent
from ray.exceptions import TaskCancelledError


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

    with pytest.raises(ray.exceptions.RayTaskError) as e:
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

    # When there are large input size in-flight actor tasks
    # tasks are queued inside a RPC layer (core_worker_client.h)
    # In this case, we don't cancel a request from a client side
    # but wait until it is sent to the server side and cancel it.
    # See SendRequests() inside core_worker_client.h
    # SANG-TODO


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
    ref = a.f.remote()
    # Queued on a server side.
    # Task should not be executed at all.
    # SANG-TODO
    ref2 = a.g.remote()
    ray.cancel(ref2)
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(ref2)
    # SANG-TODO assert there's no g because it hasn't even run.
    # RUNNING events shouldn't exist.
    print(list_task())


def test_async_actor_cancel_after_task_finishes(shutdown_only):
    @ray.remote
    class Actor:
        async def f(self):
            await asyncio.sleep(5)

        async def emtpy(self):
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
        async def sleep(self, sg):
            await asyncio.sleep(1000)

    @ray.remote
    def f(refs):
        ref = refs[0]
        ray.cancel(ref)

    a = Actor.remote()
    sleep_ref = a.sleep.remote()
    ref = f.remote([a.sleep.remote([sleep_ref])])

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(sleep_ref)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
