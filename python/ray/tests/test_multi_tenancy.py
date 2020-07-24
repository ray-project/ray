# coding: utf-8
import json
import os
import sys
from multiprocessing import Process, Queue

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


def test_multi_drivers(shutdown_only):
    def run_driver(queue, results_queue):
        ray.init(address=queue.get())

        @ray.remote
        class Actor:
            def get_pid(self):
                return os.getpid()

        @ray.remote
        def get_pid():
            return os.getpid()

        pid_objs = []
        pid_objs = pid_objs + [get_pid.remote() for _ in range(100)]
        actors = [Actor.remote() for _ in range(10)]
        pid_objs = pid_objs + [actor.get_pid.remote() for actor in actors]

        pids = set([ray.get(obj) for obj in pid_objs])
        results_queue.put((os.getpid(), pids))

        ray.shutdown()

    driver_count = 10
    queue = Queue()
    results_queue = Queue()
    processes = [
        Process(target=run_driver, args=(queue, results_queue))
        for _ in range(driver_count)
    ]
    for p in processes:
        p.start()

    info = ray.init(
        _internal_config=json.dumps({
            "enable_multi_tenancy": True
        }))
    for _ in range(driver_count):
        queue.put(info["redis_address"])

    for p in processes:
        p.join(timeout=60)
        assert not p.is_alive()
        assert p.exitcode == 0

    all_worker_pids = set()
    for _ in range(driver_count):
        driver_pid, worker_pids = results_queue.get()
        for worker_pid in worker_pids:
            assert worker_pid not in all_worker_pids, (
                "Worker process with PID {} is shared by multiple drivers." %
                worker_pid)
            all_worker_pids.add(worker_pid)


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
