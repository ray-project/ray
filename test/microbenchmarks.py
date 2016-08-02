import unittest
import ray
import time
import os
import numpy as np

import test_functions

class MicroBenchmarkTest(unittest.TestCase):

  def testTiming(self):
    reload(test_functions)
    ray.init(start_ray_local=True, num_workers=3)

    # measure the time required to submit a remote task to the scheduler
    elapsed_times = []
    for _ in range(1000):
      start_time = time.time()
      test_functions.empty_function.remote()
      end_time = time.time()
      elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print "Time required to submit an empty function call:"
    print "    Average: {}".format(average_elapsed_time)
    print "    90th percentile: {}".format(elapsed_times[900])
    print "    99th percentile: {}".format(elapsed_times[990])
    print "    worst:           {}".format(elapsed_times[999])
    # average_elapsed_time should be about 0.00038

    # measure the time required to submit a remote task to the scheduler (where the remote task returns one value)
    elapsed_times = []
    for _ in range(1000):
      start_time = time.time()
      test_functions.trivial_function.remote()
      end_time = time.time()
      elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print "Time required to submit a trivial function call:"
    print "    Average: {}".format(average_elapsed_time)
    print "    90th percentile: {}".format(elapsed_times[900])
    print "    99th percentile: {}".format(elapsed_times[990])
    print "    worst:           {}".format(elapsed_times[999])
    # average_elapsed_time should be about 0.001

    # measure the time required to submit a remote task to the scheduler and get the result
    elapsed_times = []
    for _ in range(1000):
      start_time = time.time()
      x = test_functions.trivial_function.remote()
      ray.get(x)
      end_time = time.time()
      elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print "Time required to submit a trivial function call and get the result:"
    print "    Average: {}".format(average_elapsed_time)
    print "    90th percentile: {}".format(elapsed_times[900])
    print "    99th percentile: {}".format(elapsed_times[990])
    print "    worst:           {}".format(elapsed_times[999])
    # average_elapsed_time should be about 0.0013

    # measure the time required to do do a put
    elapsed_times = []
    for _ in range(1000):
      start_time = time.time()
      ray.put(1)
      end_time = time.time()
      elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print "Time required to put an int:"
    print "    Average: {}".format(average_elapsed_time)
    print "    90th percentile: {}".format(elapsed_times[900])
    print "    99th percentile: {}".format(elapsed_times[990])
    print "    worst:           {}".format(elapsed_times[999])
    # average_elapsed_time should be about 0.00087

    ray.worker.cleanup()

if __name__ == "__main__":
    unittest.main()
