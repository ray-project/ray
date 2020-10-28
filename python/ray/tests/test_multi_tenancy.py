# coding: utf-8
import os
import sys
import time

import grpc
import pytest

import ray
import ray.test_utils
from ray.core.generated import common_pb2
from ray.core.generated import node_manager_pb2, node_manager_pb2_grpc
from ray.test_utils import (wait_for_condition, wait_for_pid_to_exit,
                            run_string_as_driver,
                            run_string_as_driver_nonblocking)


def get_workers():
    raylet = ray.nodes()[0]
    raylet_address = "{}:{}".format(raylet["NodeManagerAddress"],
                                    raylet["NodeManagerPort"])
    channel = grpc.insecure_channel(raylet_address)
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
    """.format(info["redis_address"])

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
        # out = ray.utils.decode(out)
        # err = ray.utils.decode(err)
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


def test_worker_env(shutdown_only):
    ray.init(
        job_config=ray.job_config.JobConfig(worker_env={
            "foo1": "bar1",
            "foo2": "bar2"
        }))

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


def test_worker_capping_fifo(shutdown_only):
    # Start 2 initial workers by setting num_cpus to 2.
    info = ray.init(num_cpus=2)
    wait_for_condition(lambda: len(get_workers()) == 2)

    time.sleep(1)

    @ray.remote
    def getpid():
        return os.getpid()

    worker1, worker2 = get_workers()

    if worker1.pid == ray.get(getpid.remote()):
        worker1, worker2 = [worker2, worker1]

    # Worker 1 is before worker 2 in the FIFO queue.

    driver_code = """
import ray
import time

ray.init(address="{}")

@ray.remote
def foo():
    pass

ray.get(foo.remote())
# Sleep a while to make sure an idle worker exits before this driver exits.
time.sleep(2)
ray.shutdown()
    """.format(info["redis_address"])

    run_string_as_driver(driver_code)

    # Worker 1 should have been killed.
    wait_for_pid_to_exit(worker1.pid)

    wait_for_condition(lambda: len(get_workers()) == 1)
    assert worker2.pid == get_workers()[0].pid


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
    """.format(info["redis_address"])

    before = len(get_workers())
    assert before == 1

    run_string_as_driver(driver_code)

    # wait for a while to let workers register
    time.sleep(2)
    wait_for_condition(lambda: len(get_workers()) == before)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
