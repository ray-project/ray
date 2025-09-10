# coding: utf-8
import os
import subprocess
import sys
import tempfile
import time
from typing import List

import numpy as np
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    run_string_as_driver,
    run_string_as_driver_nonblocking,
)
from ray.util.state import list_workers
from ray.util.state.common import WorkerState


def get_workers() -> List[WorkerState]:
    """Return non-driver workers."""
    return list_workers(
        filters=[("worker_type", "=", "WORKER"), ("is_alive", "=", "True")]
    )


# Test that when `redis_address` and `job_config` is not set in
# `ray.init(...)`, Raylet will start `num_cpus` Python workers for the driver.
def test_initial_workers(shutdown_only):
    ray.init(num_cpus=2)
    wait_for_condition(lambda: len(get_workers()) == 2)


# This test case starts some driver processes. Each driver process submits
# some tasks and collect the PIDs of the workers used by the driver. The
# drivers output the PID list which will be read by the test case itself. The
# test case will compare the PIDs used by different drivers and make sure that
# all the PIDs don't overlap. If overlapped, it means that tasks owned by
# different drivers were scheduled to the same worker process, that is, tasks
# of different jobs were not correctly isolated during execution.
def test_multi_drivers(shutdown_only):
    info = ray.init(num_cpus=10)

    driver_code = """
import os
import sys
import ray


ray.init(address="{}")

@ray.remote
class Actor:
    def get_pid(self):
        return os.getpid()

@ray.remote
def get_pid():
    return os.getpid()

pid_objs = []
# Submit some normal tasks and get the PIDs of workers which execute the tasks.
pid_objs = pid_objs + [get_pid.remote() for _ in range(2)]
# Create some actors and get the PIDs of actors.
actors = [Actor.remote() for _ in range(2)]
pid_objs = pid_objs + [actor.get_pid.remote() for actor in actors]

pids = set([ray.get(obj) for obj in pid_objs])
# Write pids to stdout
print("PID:" + str.join(",", [str(_) for _ in pids]))

ray.shutdown()
    """.format(
        info["address"]
    )

    driver_count = 3
    processes = [
        run_string_as_driver_nonblocking(driver_code) for _ in range(driver_count)
    ]
    outputs = []
    for p in processes:
        out = p.stdout.read().decode("ascii")
        err = p.stderr.read().decode("ascii")
        p.wait()
        if p.returncode != 0:
            print(
                "Driver with PID {} returned error code {}".format(p.pid, p.returncode)
            )
            print("STDOUT:\n{}".format(out))
            print("STDERR:\n{}".format(err))
        outputs.append((p, out))

    all_worker_pids = set()
    for p, out in outputs:
        assert p.returncode == 0
        for line in out.splitlines():
            if line.startswith("PID:"):
                worker_pids = [int(_) for _ in line.split(":")[1].split(",")]
                assert len(worker_pids) > 0
                for worker_pid in worker_pids:
                    assert worker_pid not in all_worker_pids, (
                        "Worker process with PID {} is shared" + " by multiple drivers."
                    ).format(worker_pid)
                    all_worker_pids.add(worker_pid)


class SignalFile:
    def __init__(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self._tmppath = os.path.join(self._tmpdir.name, "signal.txt")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._tmpdir.cleanup()

    def wait(self):
        while not os.path.exists(self._tmppath):
            time.sleep(0.1)

    def send(self):
        with open(self._tmppath, "w") as f:
            f.write("go!")
            f.flush()
            f.close()


def test_kill_idle_workers(shutdown_only):
    # Avoid starting initial workers by setting num_cpus to 0.
    ray.init(num_cpus=0)
    assert len(get_workers()) == 0

    @ray.remote(num_cpus=0)
    class Actor:
        pass

    # Worker 1 should be alive running the actor.
    a = Actor.remote()
    ray.get(a.__ray_ready__.remote())
    assert len(get_workers()) == 1

    # NOTE(edoakes): I tried writing this test using a SignalActor instead of a file
    # to coordinate the tasks, but it failed because the idle workers weren't killed.
    with SignalFile() as signal:

        @ray.remote(num_cpus=0)
        def foo():
            signal.wait()

        # Worker 2 should be alive running foo.
        obj1 = foo.remote()
        wait_for_condition(lambda: len(get_workers()) == 2)

        # Worker 3 should be alive running foo.
        obj2 = foo.remote()
        wait_for_condition(lambda: len(get_workers()) == 3)

        # Signal the tasks to unblock and wait for them to complete.
        signal.send()
        ray.get([obj1, obj2])

    # Worker 2 and 3 now become idle and should be killed.
    wait_for_condition(lambda: len(get_workers()) == 1)

    # Worker 1 should also be killed when the actor exits.
    del a
    wait_for_condition(lambda: len(get_workers()) == 0)


def test_worker_capping_run_many_small_tasks(shutdown_only):
    ray.init(num_cpus=2)

    with SignalFile() as signal:

        @ray.remote(num_cpus=0.5)
        def foo():
            signal.wait()

        # Run more tasks than `num_cpus`, but the CPU resource requirement is
        # still within `num_cpus`.
        obj_refs = [foo.remote() for _ in range(4)]
        wait_for_condition(lambda: len(get_workers()) == 4)

        # Unblock the tasks.
        signal.send()
        ray.get(obj_refs)

    # After the tasks finish, some workers are killed to keep the total
    # number of workers <= num_cpus.
    wait_for_condition(lambda: len(get_workers()) == 2)

    # The two remaining workers stay alive forever.
    for _ in range(10):
        assert len(get_workers()) == 2


def test_worker_capping_run_chained_tasks(shutdown_only):
    ray.init(num_cpus=2)

    with SignalFile() as signal:

        @ray.remote(num_cpus=0.5)
        def foo(x):
            if x > 1:
                return ray.get(foo.remote(x - 1)) + x
            else:
                signal.wait()
                return x

        # Run a chain of tasks which exceed `num_cpus` in amount, but the CPU
        # resource requirement is still within `num_cpus`.
        obj = foo.remote(4)
        wait_for_condition(lambda: len(get_workers()) == 4)

        # Unblock the tasks.
        signal.send()
        ray.get(obj)

    # After finished the tasks, some workers are killed to keep the total
    # number of workers <= num_cpus.
    wait_for_condition(lambda: len(get_workers()) == 2)

    # The two remaining workers stay alive forever.
    for _ in range(10):
        assert len(get_workers()) == 2


def test_worker_registration_failure_after_driver_exit(shutdown_only):
    info = ray.init(num_cpus=2)
    wait_for_condition(lambda: len(get_workers()) == 2)

    driver_code = """
import os
import ray
import time


ray.init(address="{}")

@ray.remote
def foo():
    pass

obj_refs = [foo.remote() for _ in range(1000)]
ray.get(obj_refs[0])
os._exit(0)
    """.format(
        info["address"]
    )

    # Run a driver that spawns many tasks and blocks until the first result is ready,
    # so at least one worker should have registered.
    try:
        run_string_as_driver(driver_code)
    except subprocess.CalledProcessError:
        # The driver exits with non-zero status Windows due to ungraceful os._exit.
        pass

    # Verify that the workers spawned by the old driver go away.
    wait_for_condition(lambda: len(get_workers()) <= 2)


def test_not_killing_workers_that_own_objects(shutdown_only):
    idle_worker_kill_interval_ms = 10

    # Set the small interval for worker capping
    # so that we can easily trigger it.
    ray.init(
        num_cpus=0,
        _system_config={
            "kill_idle_workers_interval_ms": idle_worker_kill_interval_ms,
        },
    )

    # Create a nested tasks to start 4 workers each of which owns an object.
    with SignalFile() as signal:
        expected_num_workers = 4

        @ray.remote(num_cpus=0)
        def nested(i):
            # Each of these tasks owns an object so it shouldn't be killed.
            if i >= expected_num_workers - 1:
                signal.wait()
                return [ray.put(np.ones(1 * 1024 * 1024, dtype=np.uint8))]
            else:
                return [ray.put(np.ones(1 * 1024 * 1024, dtype=np.uint8))] + ray.get(
                    nested.remote(i + 1)
                )

        # Wait for all the workers to start up.
        outer_ref = nested.remote(0)
        wait_for_condition(lambda: len(get_workers()) == expected_num_workers)

        # Unblock the tasks.
        signal.send()
        inner_ref = ray.get(outer_ref)

    # Sleep for 10x the idle worker kill interval and verify that those workers
    # aren't killed because they own objects that are in scope.
    time.sleep((10 * idle_worker_kill_interval_ms) / 1000.0)
    assert len(get_workers()) == expected_num_workers
    del inner_ref


def test_kill_idle_workers_that_are_behind_owned_workers(shutdown_only):
    # When the first N idle workers own objects, and if we have N+N
    # total idle workers, we should make sure other N workers are killed.
    # It is because the idle workers are killed in the FIFO order.
    N = 4
    ray.init(
        num_cpus=1,
        _system_config={
            "kill_idle_workers_interval_ms": 10,
            "worker_lease_timeout_milliseconds": 0,
        },
    )

    @ray.remote
    def nested(i):
        if i >= (N * 2) - 1:
            return [ray.put(np.ones(1 * 1024 * 1024, dtype=np.uint8))]
        elif i >= N:
            return [ray.put(np.ones(1 * 1024 * 1024, dtype=np.uint8))] + ray.get(
                nested.remote(i + 1)
            )
        else:
            return [1] + ray.get(nested.remote(i + 1))

    # The first N workers don't own objects
    # and the later N workers do.
    ref = ray.get(nested.remote(0))
    assert len(ref) == N * 2
    num_workers = len(get_workers())
    assert num_workers == N * 2

    # Make sure there are only N workers left after worker capping.
    wait_for_condition(lambda: len(get_workers()) == N)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
