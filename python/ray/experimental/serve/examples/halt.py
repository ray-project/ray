from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import ray
from ray.experimental.serve import RayServeMixin, batched_input


@ray.remote
class SleepOnFirst(RayServeMixin):
    """Sleep on the first request, return batch size.

    Used for testing the DeadlineAwareRouter.
    """

    def __init__(self, sleep_time):
        self.nap_time = sleep_time

    @batched_input
    def __call__(self, input_batch):
        time.sleep(self.nap_time)
        return [len(input_batch) for _ in range(len(input_batch))]


@ray.remote
class SleepCounter(RayServeMixin):
    """Sleep on input argument seconds, return the query id.

    Used to test the DeadlineAwareRouter.
    """

    def __init__(self):
        self.counter = 0

    def __call__(self, inp):
        time.sleep(inp)

        self.counter += 1
        return self.counter
