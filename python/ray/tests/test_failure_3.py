import threading
import os
import sys
import random
import string

import ray

import numpy as np
import pytest
import time

from ray._private.test_utils import SignalActor
from ray.data.impl.progress_bar import ProgressBar


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "num_cpus": 5,
        "object_store_memory": 10**8,
    }],
    indirect=True)
def test_parallel_actor_fill_plasma_retry(ray_start_cluster_head):
    @ray.remote
    class LargeMemoryActor:
        def some_expensive_task(self):
            return np.zeros(10**8 // 2, dtype=np.uint8)

    actors = [LargeMemoryActor.remote() for _ in range(5)]
    for _ in range(5):
        pending = [a.some_expensive_task.remote() for a in actors]
        while pending:
            [done], pending = ray.wait(pending, num_returns=1)


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "_system_config": {
            "task_retry_delay_ms": 500
        }
    }],
    indirect=True)
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
        time.sleep(.1)
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


def test_task_retry_mini_integration(ray_start_cluster):
    """Test nested tasks with infinite retry and
        keep killing nodes while retrying is happening.

        It is the sanity check test for larger scale chaos testing.
    """
    cluster = ray_start_cluster
    NUM_NODES = 3
    NUM_CPUS = 8
    # head node.
    cluster.add_node(num_cpus=0, resources={"head": 1})
    ray.init(address=cluster.address)
    workers = []
    for _ in range(NUM_NODES):
        workers.append(
            cluster.add_node(num_cpus=NUM_CPUS, resources={"worker": 1}))

    @ray.remote(max_retries=-1, resources={"worker": 0.1})
    def task():
        def generate_data(size_in_kb=10):
            return np.zeros(1024 * size_in_kb, dtype=np.uint8)

        a = ""
        for _ in range(100000):
            a = a + random.choice(string.ascii_letters)
        return generate_data(size_in_kb=50)

    @ray.remote(max_retries=-1, resources={"worker": 0.1})
    def invoke_nested_task():
        time.sleep(0.8)
        return ray.get(task.remote())

    # 50MB of return values.
    TOTAL_TASKS = 500

    def run_chaos_test():
        # Chaos testing.
        pb = ProgressBar("Chaos test sanity check", TOTAL_TASKS)
        results = [invoke_nested_task.remote() for _ in range(TOTAL_TASKS)]
        start = time.time()
        pb.block_until_complete(results)
        runtime_with_failure = time.time() - start
        print(f"Runtime when there are many failures: {runtime_with_failure}")
        pb.close()

    x = threading.Thread(target=run_chaos_test)
    x.start()

    kill_interval = 2
    start = time.time()
    while True:
        worker_to_kill = workers.pop(0)
        pid = worker_to_kill.all_processes["raylet"][0].process.pid
        # SIGKILL
        os.kill(pid, 9)
        workers.append(
            cluster.add_node(num_cpus=NUM_CPUS, resources={"worker": 1}))
        time.sleep(kill_interval)
        if time.time() - start > 30:
            break
    x.join()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
