"""
This file contains example ray servable actors. These actors
are used for testing as well as demoing purpose.
"""

from __future__ import absolute_import, division, print_function

import time

import numpy as np

import ray
from ray.experimental.serve import RayServeMixin, batched_input


@ray.remote
class VectorizedAdder(RayServeMixin):
    """Actor that adds scaler_increment to input batch.

    result = np.array(input_batch) + scaler_increment
    """

    def __init__(self, scaler_increment):
        self.inc = scaler_increment

    @batched_input
    def __call__(self, input_batch):
        arr = np.array(input_batch)
        arr += self.inc
        return arr.tolist()


@ray.remote
class ScalerAdder(RayServeMixin):
    """Actor that adds a scaler_increment to a single input."""

    def __init__(self, scaler_increment):
        self.inc = scaler_increment

    def __call__(self, input_scaler):
        return input_scaler + self.inc


@ray.remote
class VectorDouble(RayServeMixin):
    """Actor that doubles the batched input."""

    @batched_input
    def __call__(self, batched_vectors):
        matrix = np.array(batched_vectors)
        matrix *= 2
        return [v.tolist() for v in matrix]


@ray.remote
class Counter(RayServeMixin):
    """Return the query id. Used for testing router."""

    def __init__(self):
        self.counter = 0

    def __call__(self, batched_input):
        self.counter += 1
        return self.counter


@ray.remote
class CustomCounter(RayServeMixin):
    """Return the query id. Used for testing `serve_method` signature."""

    serve_method = "count"

    @batched_input
    def count(self, input_batch):
        return [1 for _ in range(len(input_batch))]


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
