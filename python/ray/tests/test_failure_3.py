import os
import sys
import signal

import ray

import numpy as np
import pytest
import time

from ray._private.test_utils import (
    SignalActor,
    wait_for_pid_to_exit,
)

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM


def test_worker_exit_after_parent_raylet_dies(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=8, resources={"foo": 1})
    cluster.wait_for_nodes()

    ray.init(address=cluster.address)

    @ray.remote(resources={"foo": 1})
    class Actor:
        def get_worker_pid(self):
            return os.getpid()

        def get_raylet_pid(self):
            return int(os.environ["RAY_RAYLET_PID"])

    actor = Actor.remote()
    worker_pid = ray.get(actor.get_worker_pid.remote())
    raylet_pid = ray.get(actor.get_raylet_pid.remote())
    # Kill the parent raylet.
    os.kill(raylet_pid, SIGKILL)
    os.waitpid(raylet_pid, 0)
    wait_for_pid_to_exit(raylet_pid)
    # Make sure the worker process exits as well.
    wait_for_pid_to_exit(worker_pid)


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "num_cpus": 5,
            "object_store_memory": 10 ** 8,
        }
    ],
    indirect=True,
)
def test_parallel_actor_fill_plasma_retry(ray_start_cluster_head):
    @ray.remote
    class LargeMemoryActor:
        def some_expensive_task(self):
            return np.zeros(10 ** 8 // 2, dtype=np.uint8)

    actors = [LargeMemoryActor.remote() for _ in range(5)]
    for _ in range(5):
        pending = [a.some_expensive_task.remote() for a in actors]
        while pending:
            [done], pending = ray.wait(pending, num_returns=1)


@pytest.mark.parametrize(
    "ray_start_regular",
    [{"_system_config": {"task_retry_delay_ms": 500}}],
    indirect=True,
)
def test_async_actor_task_retries(ray_start_regular):
    # https://github.com/ray-project/ray/issues/11683

    signal = SignalActor.remote()

    @ray.remote
    class DyingActor:
        def __init__(self):
            print("DyingActor init called")
            self.should_exit = False

        def set_should_exit(self):
            print("DyingActor.set_should_exit called")
            self.should_exit = True

        async def get(self, x, wait=False):
            print(f"DyingActor.get called with x={x}, wait={wait}")
            if self.should_exit:
                os._exit(0)
            if wait:
                await signal.wait.remote()
            return x

    # Normal in order actor task retries should work
    dying = DyingActor.options(
        max_restarts=-1,
        max_task_retries=-1,
    ).remote()

    assert ray.get(dying.get.remote(1)) == 1
    ray.get(dying.set_should_exit.remote())
    assert ray.get(dying.get.remote(42)) == 42

    # Now let's try out of order retries:
    # Task seqno 0 will return
    # Task seqno 1 will be pending and retried later
    # Task seqno 2 will return
    # Task seqno 3 will crash the actor and retried later
    dying = DyingActor.options(
        max_restarts=-1,
        max_task_retries=-1,
    ).remote()

    # seqno 0
    ref_0 = dying.get.remote(0)
    assert ray.get(ref_0) == 0
    # seqno 1
    ref_1 = dying.get.remote(1, wait=True)
    # Need a barrier here to ensure ordering between the async and sync call.
    # Otherwise ref2 could be executed prior to ref1.
    for i in range(100):
        if ray.get(signal.cur_num_waiters.remote()) > 0:
            break
        time.sleep(0.1)
    assert ray.get(signal.cur_num_waiters.remote()) > 0
    # seqno 2
    ref_2 = dying.set_should_exit.remote()
    assert ray.get(ref_2) is None
    # seqno 3, this will crash the actor because previous task set should exit
    # to true.
    ref_3 = dying.get.remote(3)

    # At this point the actor should be restarted. The two pending tasks
    # [ref_1, ref_3] should be retried, but not the completed tasks [ref_0,
    # ref_2]. Critically, if ref_2 was retried, ref_3 can never return.
    ray.get(signal.send.remote())
    assert ray.get(ref_1) == 1
    assert ray.get(ref_3) == 3


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
