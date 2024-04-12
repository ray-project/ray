import os
import sys
import signal
import threading
import json
from pathlib import Path

import ray
import numpy as np
import pytest
import psutil
import time

from ray._private.test_utils import (
    SignalActor,
    wait_for_pid_to_exit,
    wait_for_condition,
    run_string_as_driver_nonblocking,
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
            "object_store_memory": 10**8,
        }
    ],
    indirect=True,
)
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


def test_actor_failure_async(ray_start_regular):
    @ray.remote
    class A:
        def echo(self):
            pass

        def pid(self):
            return os.getpid()

    a = A.remote()
    rs = []

    def submit():
        for i in range(10000):
            r = a.echo.remote()
            r._on_completed(lambda x: 1)
            rs.append(r)

    t = threading.Thread(target=submit)
    pid = ray.get(a.pid.remote())

    t.start()
    from time import sleep

    sleep(0.1)
    os.kill(pid, SIGKILL)

    t.join()


@pytest.mark.parametrize(
    "ray_start_regular",
    [{"_system_config": {"timeout_ms_task_wait_for_death_info": 100000000}}],
    indirect=True,
)
def test_actor_failure_async_2(ray_start_regular, tmp_path):
    p = tmp_path / "a_pid"

    @ray.remote(max_restarts=1)
    class A:
        def __init__(self):
            pid = os.getpid()
            # The second time start, it'll block,
            # so that we'll know the actor is restarting.
            if p.exists():
                p.write_text(str(pid))
                time.sleep(100000)
            else:
                p.write_text(str(pid))

        def pid(self):
            return os.getpid()

    a = A.remote()

    pid = ray.get(a.pid.remote())

    os.kill(int(pid), SIGKILL)

    # kill will be in another thred.
    def kill():
        # sleep for 2s for the code to be setup
        time.sleep(2)
        new_pid = int(p.read_text())
        while new_pid == pid:
            new_pid = int(p.read_text())
            time.sleep(1)
        os.kill(new_pid, SIGKILL)

    t = threading.Thread(target=kill)
    t.start()

    try:
        o = a.pid.remote()

        def new_task(_):
            print("new_task")
            # make sure there is no deadlock
            a.pid.remote()

        o._on_completed(new_task)
        # When ray.get(o) failed,
        # new_task will be executed
        ray.get(o)
    except Exception:
        pass
    t.join()


@pytest.mark.parametrize(
    "ray_start_regular",
    [{"_system_config": {"timeout_ms_task_wait_for_death_info": 100000000}}],
    indirect=True,
)
def test_actor_failure_async_3(ray_start_regular):
    @ray.remote(max_restarts=1)
    class A:
        def pid(self):
            return os.getpid()

    a = A.remote()

    def new_task(_):
        print("new_task")
        # make sure there is no deadlock
        a.pid.remote()

    t = a.pid.remote()
    # Make sure there is no deadlock when executing
    # the callback
    t._on_completed(new_task)

    ray.kill(a)

    with pytest.raises(Exception):
        ray.get(t)


@pytest.mark.parametrize(
    "ray_start_regular",
    [{"_system_config": {"timeout_ms_task_wait_for_death_info": 100000000}}],
    indirect=True,
)
def test_actor_failure_async_4(ray_start_regular, tmp_path):
    from filelock import FileLock

    l_file = tmp_path / "lock"

    l_lock = FileLock(l_file)
    l_lock.acquire()

    @ray.remote
    def f():
        with FileLock(l_file):
            os.kill(os.getpid(), SIGKILL)

    @ray.remote(max_restarts=1)
    class A:
        def pid(self, x):
            return os.getpid()

    a = A.remote()

    def new_task(_):
        print("new_task")
        # make sure there is no deadlock
        a.pid.remote(None)

    t = a.pid.remote(f.remote())
    # Make sure there is no deadlock when executing
    # the callback
    t._on_completed(new_task)

    ray.kill(a)

    # This will make the dependence failed
    l_lock.release()

    with pytest.raises(Exception):
        ray.get(t)


@pytest.mark.parametrize(
    "ray_start_regular",
    [
        {
            "_system_config": {
                "timeout_ms_task_wait_for_death_info": 0,
                "core_worker_internal_heartbeat_ms": 1000000,
            }
        }
    ],
    indirect=True,
)
def test_actor_failure_no_wait(ray_start_regular, tmp_path):
    p = tmp_path / "a_pid"
    time.sleep(1)

    # Make sure the request will fail immediately without waiting for the death info
    @ray.remote(max_restarts=1, max_task_retries=0)
    class A:
        def __init__(self):
            pid = os.getpid()
            # The second time start, it'll block,
            # so that we'll know the actor is restarting.
            if p.exists():
                p.write_text(str(pid))
                time.sleep(100000)
            else:
                p.write_text(str(pid))

        def p(self):
            time.sleep(100000)

        def pid(self):
            return os.getpid()

    a = A.remote()
    pid = ray.get(a.pid.remote())
    t = a.p.remote()
    os.kill(int(pid), SIGKILL)
    with pytest.raises(ray.exceptions.RayActorError):
        # Make sure it'll return within 1s
        ray.get(t)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_no_worker_child_process_leaks(ray_start_cluster, tmp_path):
    """
    Verify that processes created by Ray tasks and actors are
    cleaned up after a Ctrl+C is sent to the driver. This is done by
    creating an actor and task that each spawn a number of child
    processes, sending a SIGINT to the driver process, and
    verifying that all child processes are killed.

    The driver script uses a temporary JSON file to communicate
    the list of PIDs that are children of the Ray worker
    processes.
    """

    output_file_path = tmp_path / "leaked_pids.json"
    ray_start_cluster.add_node()
    driver_script = f"""
import ray
import json
import multiprocessing
import shutil
import time
import os

@ray.remote
class Actor:
    def create_leaked_child_process(self, num_to_leak):
        print("Creating leaked process", os.getpid())

        pids = []
        for _ in range(num_to_leak):
            proc = multiprocessing.Process(
                target=time.sleep,
                args=(1000,),
                daemon=True,
            )
            proc.start()
            pids.append(proc.pid)

        return pids

@ray.remote
def task():
    print("Creating leaked process", os.getpid())
    proc = multiprocessing.Process(
        target=time.sleep,
        args=(1000,),
        daemon=True,
    )
    proc.start()

    return proc.pid

num_to_leak_per_type = 10

actor = Actor.remote()
actor_leaked_pids = ray.get(actor.create_leaked_child_process.remote(
    num_to_leak=num_to_leak_per_type,
))

task_leaked_pids = ray.get([task.remote() for _ in range(num_to_leak_per_type)])
leaked_pids = actor_leaked_pids + task_leaked_pids

final_file = "{output_file_path}"
tmp_file = final_file + ".tmp"
with open(tmp_file, "w") as f:
    json.dump(leaked_pids, f)
shutil.move(tmp_file, final_file)

while True:
    print(os.getpid())
    time.sleep(1)
    """

    driver_proc = run_string_as_driver_nonblocking(driver_script)

    # Wait for the json file containing the child PIDS
    # to be present.
    wait_for_condition(
        condition_predictor=lambda: Path(output_file_path).exists(),
        timeout=30,
    )

    # Load the PIDs of the child processes.
    with open(output_file_path, "r") as f:
        pids = json.load(f)

    # Validate all children of the worker processes are in a sleeping state.
    processes = [psutil.Process(pid) for pid in pids]
    assert all([proc.status() == psutil.STATUS_SLEEPING for proc in processes])

    # Valdiate children of worker process die after SIGINT.
    driver_proc.send_signal(signal.SIGINT)
    wait_for_condition(
        condition_predictor=lambda: all([not proc.is_running() for proc in processes]),
        timeout=30,
    )


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_worker_cleans_up_child_procs_on_raylet_death(ray_start_cluster, tmp_path):
    """
    CoreWorker kills its child processes if the raylet dies.
    This test creates 20 leaked processes; 10 from a single actor task, and
    10 from distinct non-actor tasks.

    Once the raylet dies, the test verifies all leaked processes are cleaned up.
    """

    output_file_path = tmp_path / "leaked_pids.json"
    ray_start_cluster.add_node()
    driver_script = f"""
import ray
import json
import multiprocessing
import shutil
import time
import os
import setproctitle

def change_name_and_sleep(label: str, index: int) -> None:
    proctitle = "child_proc_name_prefix_" + label + "_" + str(index)
    setproctitle.setproctitle(proctitle)
    time.sleep(1000)

def create_child_proc(label, index):
    proc = multiprocessing.Process(
        target=change_name_and_sleep,
        args=(label, index,),
        daemon=True,
    )
    proc.start()
    return proc.pid

@ray.remote
class LeakerActor:
    def create_leaked_child_process(self, num_to_leak):
        print("creating leaked process", os.getpid())

        pids = []
        for index in range(num_to_leak):
            pid = create_child_proc("actor", index)
            pids.append(pid)

        return pids

@ray.remote
def leaker_task(index):
    print("Creating leaked process", os.getpid())
    return create_child_proc("task", index)

num_to_leak_per_type = 10
print('starting actors')
actor = LeakerActor.remote()
actor_leaked_pids = ray.get(actor.create_leaked_child_process.remote(
    num_to_leak=num_to_leak_per_type,
))

task_leaked_pids = ray.get([
    leaker_task.remote(index) for index in range(num_to_leak_per_type)
])
leaked_pids = actor_leaked_pids + task_leaked_pids

final_file = "{output_file_path}"
tmp_file = final_file + ".tmp"
with open(tmp_file, "w") as f:
    json.dump(leaked_pids, f)
shutil.move(tmp_file, final_file)

while True:
    print(os.getpid())
    time.sleep(1)
    """

    print("Running string as driver")
    driver_proc = run_string_as_driver_nonblocking(driver_script)

    # Wait for the json file containing the child PIDS
    # to be present.
    print("Waiting for child pids json")
    wait_for_condition(
        condition_predictor=lambda: Path(output_file_path).exists(),
        timeout=30,
    )

    # Load the PIDs of the child processes.
    with open(output_file_path, "r") as f:
        pids = json.load(f)

    # Validate all children of the worker processes are in a sleeping state.
    processes = [psutil.Process(pid) for pid in pids]
    assert all([proc.status() == psutil.STATUS_SLEEPING for proc in processes])

    # Obtain psutil handle for raylet process
    raylet_proc = [p for p in psutil.process_iter() if p.name() == "raylet"]
    assert len(raylet_proc) == 1
    raylet_proc = raylet_proc[0]

    # Kill the raylet process and reap the zombie
    raylet_proc.kill()
    raylet_proc.wait()

    print("Waiting for child procs to die")
    wait_for_condition(
        condition_predictor=lambda: all([not proc.is_running() for proc in processes]),
        timeout=30,
    )

    driver_proc.kill()


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
