from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import ray


def setup(*args):
    if not hasattr(setup, "is_initialized"):
        ray.init(num_cpus=4)
        setup.is_initialized = True


@ray.remote
def sleep(x):
    time.sleep(x)


class WaitSuite(object):
    timeout = 0.01
    timer = time.time

    def time_wait_task(self):
        ray.wait([sleep.remote(0.1)])

    def time_wait_many_tasks(self, num_returns):
        tasks = [sleep.remote(i / 5) for i in range(4)]
        ray.wait(tasks, num_returns=num_returns)

    time_wait_many_tasks.params = list(range(1, 4))
    time_wait_many_tasks.param_names = ["num_returns"]

    def time_wait_timeout(self, timeout):
        ray.wait([sleep.remote(0.5)], timeout=timeout)

    time_wait_timeout.params = [0.2, 0.8]
    time_wait_timeout.param_names = ["timeout"]
