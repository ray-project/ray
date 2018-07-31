from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import multiprocessing
import os
import subprocess
import time
import unittest

import ray

from ray.test.test_utils import run_and_get_output


class MonitorTest(unittest.TestCase):
    def _testCleanupOnDriverExit(self, num_redis_shards):
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

            # Two new objects.
            ray.get(ray.put(1111))
            ray.get(ray.put(1111))
            if (2, 1, summary_start[2]) != StateSummary():
                success.value = False

            @ray.remote
            def f():
                ray.put(1111)  # Yet another object.
                return 1111  # A returned object as well.

            # 1 new function.
            if (2, 1, summary_start[2] + 1) != StateSummary():
                success.value = False

            ray.get(f.remote())
            if (4, 2, summary_start[2] + 1) != StateSummary():
                success.value = False

            ray.shutdown()

        success = multiprocessing.Value('b', False)
        driver = multiprocessing.Process(target=Driver, args=(success, ))
        driver.start()
        # Wait for client to exit.
        driver.join()
        time.sleep(5)

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

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False),
        "Failing with the new GCS API.")
    def testCleanupOnDriverExitSingleRedisShard(self):
        self._testCleanupOnDriverExit(num_redis_shards=1)

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False),
        "Hanging with the new GCS API.")
    def testCleanupOnDriverExitManyRedisShards(self):
        self._testCleanupOnDriverExit(num_redis_shards=5)
        self._testCleanupOnDriverExit(num_redis_shards=31)


if __name__ == "__main__":
    unittest.main(verbosity=2)
