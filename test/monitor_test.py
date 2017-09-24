from __future__ import absolute_import, division, print_function

import multiprocessing
import subprocess
import time
import unittest

import ray


class MonitorTest(unittest.TestCase):
    def testCleanupOnDriverExit(self):
        stdout = subprocess.check_output([
            "ray",
            "start",
            "--head",
        ]).decode("ascii")
        lines = [m.strip() for m in stdout.split("\n")]
        init_cmd = [m for m in lines if m.startswith("ray.init")]
        self.assertEqual(1, len(init_cmd))
        redis_addr = init_cmd[0].split("redis_address=\"")[-1][:-2]

        def StateSummary():
            obj_tbl_len = len(ray.global_state.object_table())
            task_tbl_len = len(ray.global_state.task_table())
            func_tbl_len = len(ray.global_state.function_table())
            return obj_tbl_len, task_tbl_len, func_tbl_len

        def Driver(success):
            success.value = True
            # Start driver.
            ray.init(redis_address=redis_addr)
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

            ray.worker.cleanup()

        success = multiprocessing.Value('b', False)
        driver = multiprocessing.Process(target=Driver, args=(success, ))
        driver.start()
        # Wait for client to exit.
        driver.join()
        time.sleep(5)

        # Just make sure Driver() is run and succeeded.
        self.assertTrue(success.value)
        # Check that objects, tasks, and functions are cleaned up.
        ray.init(redis_address=redis_addr)
        self.assertEqual((0, 1), StateSummary()[:2])

        ray.worker.cleanup()
        subprocess.Popen(["ray", "stop"]).wait()


if __name__ == "__main__":
    unittest.main(verbosity=2)
