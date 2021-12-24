# coding: utf-8
import os
import sys
import time

import pytest
import numpy as np

import ray
from ray.core.generated import common_pb2
from ray.core.generated import node_manager_pb2, node_manager_pb2_grpc
from ray._private.test_utils import (wait_for_condition, run_string_as_driver,
                                     run_string_as_driver_nonblocking)
from ray._private.utils import init_grpc_channel


def get_workers():
    raylet = ray.nodes()[0]
    raylet_address = "{}:{}".format(raylet["NodeManagerAddress"],
                                    raylet["NodeManagerPort"])
    channel = init_grpc_channel(raylet_address)
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    return [
        worker for worker in stub.GetNodeStats(
            node_manager_pb2.GetNodeStatsRequest()).core_workers_stats
        if worker.worker_type != common_pb2.DRIVER
    ]


# Test that when `redis_address` and `job_config` is not set in
# `ray.init(...)`, Raylet will start `num_cpus` Python workers for the driver.
def test_initial_workers(shutdown_only):
    # `num_cpus` should be <=2 because a Travis CI machine only has 2 CPU cores
    ray.init(num_cpus=1, include_dashboard=True)
    wait_for_condition(lambda: len(get_workers()) == 1)


# This test case starts some driver processes. Each driver process submits
# some tasks and collect the PIDs of the workers used by the driver. The
# drivers output the PID list which will be read by the test case itself. The
# test case will compare the PIDs used by different drivers and make sure that
# all the PIDs don't overlap. If overlapped, it means that tasks owned by
# different drivers were scheduled to the same worker process, that is, tasks
# of different jobs were not correctly isolated during execution.
@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
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
    """.format(info["address"])

    driver_count = 3
    processes = [
        run_string_as_driver_nonblocking(driver_code)
        for _ in range(driver_count)
    ]
    outputs = []
    for p in processes:
        out = p.stdout.read().decode("ascii")
        err = p.stderr.read().decode("ascii")
        p.wait()
        # out, err = p.communicate()
        # out = ray._private.utils.decode(out)
        # err = ray._private.utils.decode(err)
        if p.returncode != 0:
            print("Driver with PID {} returned error code {}".format(
                p.pid, p.returncode))
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
                        ("Worker process with PID {} is shared" +
                         " by multiple drivers.").format(worker_pid))
                    all_worker_pids.add(worker_pid)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_runtime_env(shutdown_only):
    ray.init(
        job_config=ray.job_config.JobConfig(
            runtime_env={"env_vars": {
                "foo1": "bar1",
                "foo2": "bar2"
            }}))

    @ray.remote
    def get_env(key):
        return os.environ.get(key)

    assert ray.get(get_env.remote("foo1")) == "bar1"
    assert ray.get(get_env.remote("foo2")) == "bar2"


def test_worker_capping_kill_idle_workers(shutdown_only):
    # Avoid starting initial workers by setting num_cpus to 0.
    ray.init(num_cpus=0)
    assert len(get_workers()) == 0

    @ray.remote(num_cpus=0)
    class Actor:
        def ping(self):
            pass

    actor = Actor.remote()
    ray.get(actor.ping.remote())
    # Actor is now alive and worker 1 which holds the actor is alive
    assert len(get_workers()) == 1

    @ray.remote(num_cpus=0)
    def foo():
        # Wait for a while
        time.sleep(10)

    obj1 = foo.remote()
    # Worker 2 runs a normal task
    wait_for_condition(lambda: len(get_workers()) == 2)

    obj2 = foo.remote()
    # Worker 3 runs a normal task
    wait_for_condition(lambda: len(get_workers()) == 3)

    ray.get([obj1, obj2])
    # Worker 2 and 3 now become idle and should be killed
    wait_for_condition(lambda: len(get_workers()) == 1)


def test_worker_capping_run_many_small_tasks(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote(num_cpus=0.5)
    def foo():
        time.sleep(5)

    # Run more tasks than `num_cpus`, but the CPU resource requirement is
    # still within `num_cpus`.
    obj_refs = [foo.remote() for _ in range(4)]
    wait_for_condition(lambda: len(get_workers()) == 4)

    ray.get(obj_refs)
    # After finished the tasks, some workers are killed to keep the total
    # number of workers <= num_cpus.
    wait_for_condition(lambda: len(get_workers()) == 2)

    time.sleep(1)
    # The two remaining workers stay alive forever.
    assert len(get_workers()) == 2


def test_worker_capping_run_chained_tasks(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote(num_cpus=0.5)
    def foo(x):
        if x > 1:
            return ray.get(foo.remote(x - 1)) + x
        else:
            time.sleep(5)
            return x

    # Run a chain of tasks which exceed `num_cpus` in amount, but the CPU
    # resource requirement is still within `num_cpus`.
    obj = foo.remote(4)
    wait_for_condition(lambda: len(get_workers()) == 4)

    ray.get(obj)
    # After finished the tasks, some workers are killed to keep the total
    # number of workers <= num_cpus.
    wait_for_condition(lambda: len(get_workers()) == 2)

    time.sleep(1)
    # The two remaining workers stay alive forever.
    assert len(get_workers()) == 2


def test_worker_registration_failure_after_driver_exit(shutdown_only):
    info = ray.init(num_cpus=1)

    driver_code = """
import ray
import time


ray.init(address="{}")

@ray.remote
def foo():
    pass

[foo.remote() for _ in range(100)]

ray.shutdown()
    """.format(info["address"])

    before = len(get_workers())
    assert before == 1

    run_string_as_driver(driver_code)

    # wait for a while to let workers register
    time.sleep(2)
    wait_for_condition(lambda: len(get_workers()) <= before)


def test_not_killing_workers_that_own_objects(shutdown_only):
    # Set the small interval for worker capping
    # so that we can easily trigger it.
    ray.init(
        num_cpus=1,
        _system_config={
            "kill_idle_workers_interval_ms": 10,
            "worker_lease_timeout_milliseconds": 0
        })

    expected_num_workers = 6
    # Create a nested tasks to start 8 workers each of which owns an object.

    @ray.remote
    def nested(i):
        # The task owns an object.
        if i >= expected_num_workers - 1:
            return [ray.put(np.ones(1 * 1024 * 1024, dtype=np.uint8))]
        else:
            return ([ray.put(np.ones(1 * 1024 * 1024, dtype=np.uint8))] +
                    ray.get(nested.remote(i + 1)))

    ref = ray.get(nested.remote(0))
    num_workers = len(get_workers())

    # Wait for worker capping. worker capping should be triggered
    # every 10 ms, but we wait long enough to avoid a flaky test.
    time.sleep(1)
    ref2 = ray.get(nested.remote(0))

    # New workers shouldn't be registered because we reused the
    # previous workers that own objects.
    cur_num_workers = len(get_workers())
    # TODO(ekl) ideally these would be exactly equal, however the test is
    # occasionally flaky with that check.
    assert abs(num_workers - cur_num_workers) < 2, \
        (num_workers, cur_num_workers)
    assert len(ref2) == expected_num_workers
    assert len(ref) == expected_num_workers


def test_kill_idle_workers_that_are_behind_owned_workers(shutdown_only):
    # When the first N idle workers own objects, and if we have N+N
    # total idle workers, we should make sure other N workers are killed.
    # It is because the idle workers are killed in the FIFO order.
    N = 4
    ray.init(
        num_cpus=1,
        _system_config={
            "kill_idle_workers_interval_ms": 10,
            "worker_lease_timeout_milliseconds": 0
        })

    @ray.remote
    def nested(i):
        if i >= (N * 2) - 1:
            return [ray.put(np.ones(1 * 1024 * 1024, dtype=np.uint8))]
        elif i >= N:
            return ([ray.put(np.ones(1 * 1024 * 1024, dtype=np.uint8))] +
                    ray.get(nested.remote(i + 1)))
        else:
            return ([1] + ray.get(nested.remote(i + 1)))

    # The first N workers don't own objects
    # and the later N workers do.
    ref = ray.get(nested.remote(0))
    assert len(ref) == N * 2
    num_workers = len(get_workers())
    assert num_workers == N * 2

    # Make sure there are only N workers left after worker capping.
    wait_for_condition(lambda: len(get_workers()) == N)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
