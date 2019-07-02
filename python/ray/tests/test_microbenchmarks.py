from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import time
import numpy as np

import ray


def test_timing(ray_start_regular):
    @ray.remote
    def empty_function():
        pass

    @ray.remote
    def trivial_function():
        return 1

    # Measure the time required to submit a remote task to the scheduler.
    elapsed_times = []
    for _ in range(1000):
        start_time = time.time()
        empty_function.remote()
        end_time = time.time()
        elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print("Time required to submit an empty function call:")
    print("    Average: {}".format(average_elapsed_time))
    print("    90th percentile: {}".format(elapsed_times[900]))
    print("    99th percentile: {}".format(elapsed_times[990]))
    print("    worst:           {}".format(elapsed_times[999]))
    # average_elapsed_time should be about 0.00038.

    # Measure the time required to submit a remote task to the scheduler
    # (where the remote task returns one value).
    elapsed_times = []
    for _ in range(1000):
        start_time = time.time()
        trivial_function.remote()
        end_time = time.time()
        elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print("Time required to submit a trivial function call:")
    print("    Average: {}".format(average_elapsed_time))
    print("    90th percentile: {}".format(elapsed_times[900]))
    print("    99th percentile: {}".format(elapsed_times[990]))
    print("    worst:           {}".format(elapsed_times[999]))
    # average_elapsed_time should be about 0.001.

    # Measure the time required to submit a remote task to the scheduler
    # and get the result.
    elapsed_times = []
    for _ in range(1000):
        start_time = time.time()
        x = trivial_function.remote()
        ray.get(x)
        end_time = time.time()
        elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print("Time required to submit a trivial function call and get the "
          "result:")
    print("    Average: {}".format(average_elapsed_time))
    print("    90th percentile: {}".format(elapsed_times[900]))
    print("    99th percentile: {}".format(elapsed_times[990]))
    print("    worst:           {}".format(elapsed_times[999]))
    # average_elapsed_time should be about 0.0013.

    # Measure the time required to do do a put.
    elapsed_times = []
    for _ in range(1000):
        start_time = time.time()
        ray.put(1)
        end_time = time.time()
        elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print("Time required to put an int:")
    print("    Average: {}".format(average_elapsed_time))
    print("    90th percentile: {}".format(elapsed_times[900]))
    print("    99th percentile: {}".format(elapsed_times[990]))
    print("    worst:           {}".format(elapsed_times[999]))
    # average_elapsed_time should be about 0.00087.


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
def test_cache(ray_start_regular):
    A = np.random.rand(1, 1000000)
    v = np.random.rand(1000000)
    A_id = ray.put(A)
    v_id = ray.put(v)
    a = time.time()
    for i in range(100):
        A.dot(v)
    b = time.time() - a
    c = time.time()
    for i in range(100):
        ray.get(A_id).dot(ray.get(v_id))
    d = time.time() - c

    if d > 1.5 * b:
        if os.getenv("TRAVIS") is None:
            raise Exception("The caching test was too slow. "
                            "d = {}, b = {}".format(d, b))
        else:
            print("WARNING: The caching test was too slow. "
                  "d = {}, b = {}".format(d, b))
