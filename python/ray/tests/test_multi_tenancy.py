# coding: utf-8
import json
import subprocess
import sys
import tempfile

import grpc
import pytest

import ray
from ray.core.generated import node_manager_pb2, node_manager_pb2_grpc
from ray.test_utils import wait_for_condition


# Test that when `redis_address` and `job_config` is not set in
# `ray.init(...)`, Raylet will start `num_cpus` Python workers for the driver.
def test_initial_workers(shutdown_only):
    # `num_cpus` should be <=2 because a Travis CI machine only has 2 CPU cores
    ray.init(
        num_cpus=1,
        include_dashboard=True,
        _internal_config=json.dumps({
            "enable_multi_tenancy": True
        }))
    raylet = ray.nodes()[0]
    raylet_address = "{}:{}".format(raylet["NodeManagerAddress"],
                                    raylet["NodeManagerPort"])
    channel = grpc.insecure_channel(raylet_address)
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    assert wait_for_condition(lambda: len([
        worker for worker in stub.GetNodeStats(
            node_manager_pb2.GetNodeStatsRequest()).workers_stats
        if not worker.is_driver
    ]) == 1,
                              timeout=10)


# This test case starts some driver processes. Each driver process submits
# some tasks and collect the PIDs of the workers used by the driver. The
# drivers output the PID list which will be read by the test case itself. The
# test case will compare the PIDs used by different drivers and make sure that
# all the PIDs don't overlap. If overlapped, it means that tasks owned by
# different drivers were scheduled to the same worker process, that is, tasks
# of different jobs were not correctly isolated during execution.
def test_multi_drivers(shutdown_only):
    info = ray.init(
        _internal_config=json.dumps({
            "enable_multi_tenancy": True
        }))

    driver_code = """
import os
import sys
import ray


assert len(sys.argv) == 2
ray.init(address=sys.argv[1])

@ray.remote
class Actor:
    def get_pid(self):
        return os.getpid()

@ray.remote
def get_pid():
    return os.getpid()

pid_objs = []
# Submit some normal tasks and get the PIDs of workers which execute the tasks.
pid_objs = pid_objs + [get_pid.remote() for _ in range(5)]
# Create some actors and get the PIDs of actors.
actors = [Actor.remote() for _ in range(5)]
pid_objs = pid_objs + [actor.get_pid.remote() for actor in actors]

pids = set([ray.get(obj) for obj in pid_objs])
# Write pids to stdout
print("PID:" + str.join(",", [str(_) for _ in pids]))

ray.shutdown()
    """

    f = tempfile.NamedTemporaryFile(suffix=".py")
    f.write(driver_code.encode("ascii"))
    f.flush()

    driver_count = 10
    cmd = [sys.executable, f.name, info["redis_address"]]
    processes = [
        subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True) for _ in range(driver_count)
    ]
    outputs = []
    for p in processes:
        out, err = p.communicate()
        if p.returncode != 0:
            print("Driver with PID {} returned error code {}".format(
                p.pid, p.returncode))
            print("STDOUT:\n{}".format(out))
            print("STDERR:\n{}".format(err))
        outputs.append((p, out))

    f.close()

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
