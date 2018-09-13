from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import multiprocessing
import os
import pytest
import subprocess
import time

import ray

from ray.test.test_utils import run_and_get_output


def _test_cleanup_on_driver_exit(num_redis_shards):
    stdout = run_and_get_output([
        "ray",
        "start",
        "--head",
        "--num-redis-shards",
        str(num_redis_shards),
    ])
    lines = [m.strip() for m in stdout.split("\n")]
    init_cmd = [m for m in lines if m.startswith("ray.init")]
    assert 1 == len(init_cmd)
    redis_address = init_cmd[0].split("redis_address=\"")[-1][:-2]

    def StateSummary():
        obj_tbl_len = len(ray.global_state.object_table())
        task_tbl_len = len(ray.global_state.task_table())
        func_tbl_len = len(ray.global_state.function_table())
        return obj_tbl_len, task_tbl_len, func_tbl_len

    def Driver(success):
        success.value = True
        # Start driver.
        ray.init(redis_address=redis_address)
        summary_start = StateSummary()
        if (0, 1) != summary_start[:2]:
            success.value = False

        max_attempts_before_failing = 100

        # Two new objects.
        ray.get(ray.put(1111))
        ray.get(ray.put(1111))
        attempts = 0
        while (2, 1, summary_start[2]) != StateSummary():
            time.sleep(0.1)
            attempts += 1
            if attempts == max_attempts_before_failing:
                success.value = False
                break

        @ray.remote
        def f():
            ray.put(1111)  # Yet another object.
            return 1111  # A returned object as well.

        # 1 new function.
        attempts = 0
        while (2, 1, summary_start[2] + 1) != StateSummary():
            time.sleep(0.1)
            attempts += 1
            if attempts == max_attempts_before_failing:
                success.value = False
                break

        ray.get(f.remote())
        attempts = 0
        while (4, 2, summary_start[2] + 1) != StateSummary():
            time.sleep(0.1)
            attempts += 1
            if attempts == max_attempts_before_failing:
                success.value = False
                break

        ray.shutdown()

    success = multiprocessing.Value('b', False)
    driver = multiprocessing.Process(target=Driver, args=(success, ))
    driver.start()
    # Wait for client to exit.
    driver.join()
    time.sleep(3)

    # Just make sure Driver() is run and succeeded. Note(rkn), if the below
    # assertion starts failing, then the issue may be that the summary
    # values computed in the Driver function are being updated slowly and
    # so the call to StateSummary() is getting outdated values. This could
    # be fixed by looping until StateSummary() returns the desired values.
    assert success.value
    # Check that objects, tasks, and functions are cleaned up.
    ray.init(redis_address=redis_address)
    # The assertion below can fail if the monitor is too slow to clean up
    # the global state.
    assert (0, 1) == StateSummary()[:2]

    ray.shutdown()
    subprocess.Popen(["ray", "stop"]).wait()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with the new GCS API.")
def test_cleanup_on_driver_exit_single_redis_shard():
    _test_cleanup_on_driver_exit(num_redis_shards=1)


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with the new GCS API.")
def test_cleanup_on_driver_exit_many_redis_shards():
    _test_cleanup_on_driver_exit(num_redis_shards=5)
    _test_cleanup_on_driver_exit(num_redis_shards=31)
