import unittest
import halo
import halo.services as services
import time
import os
import numpy as np

import test_functions

class MicroBenchmarkTest(unittest.TestCase):

  def testTiming(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "test_worker.py")
    services.start_singlenode_cluster(return_drivers=False, num_workers_per_objstore=3, worker_path=test_path)

    # measure the time required to submit a remote task to the scheduler
    elapsed_times = []
    for _ in range(1000):
      start_time = time.time()
      test_functions.empty_function()
      end_time = time.time()
      elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print "Time required to submit an empty function call:"
    print "    Average: {}".format(average_elapsed_time)
    print "    90th percentile: {}".format(elapsed_times[900])
    print "    99th percentile: {}".format(elapsed_times[990])
    print "    worst:           {}".format(elapsed_times[999])
    self.assertTrue(average_elapsed_time < 0.0007) # should take 0.00038

    # measure the time required to submit a remote task to the scheduler (where the remote task returns one value)
    elapsed_times = []
    for _ in range(1000):
      start_time = time.time()
      test_functions.trivial_function()
      end_time = time.time()
      elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print "Time required to submit a trivial function call:"
    print "    Average: {}".format(average_elapsed_time)
    print "    90th percentile: {}".format(elapsed_times[900])
    print "    99th percentile: {}".format(elapsed_times[990])
    print "    worst:           {}".format(elapsed_times[999])
    self.assertTrue(average_elapsed_time < 0.002) # should take 0.001

    # measure the time required to submit a remote task to the scheduler and pull the result
    elapsed_times = []
    for _ in range(1000):
      start_time = time.time()
      x = test_functions.trivial_function()
      halo.pull(x)
      end_time = time.time()
      elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print "Time required to submit a trivial function call and pull the result:"
    print "    Average: {}".format(average_elapsed_time)
    print "    90th percentile: {}".format(elapsed_times[900])
    print "    99th percentile: {}".format(elapsed_times[990])
    print "    worst:           {}".format(elapsed_times[999])
    self.assertTrue(average_elapsed_time < 0.002) # should take 0.0013

    # measure the time required to do do a push
    elapsed_times = []
    for _ in range(1000):
      start_time = time.time()
      halo.push(1)
      end_time = time.time()
      elapsed_times.append(end_time - start_time)
    elapsed_times = np.sort(elapsed_times)
    average_elapsed_time = sum(elapsed_times) / 1000
    print "Time required to push an int:"
    print "    Average: {}".format(average_elapsed_time)
    print "    90th percentile: {}".format(elapsed_times[900])
    print "    99th percentile: {}".format(elapsed_times[990])
    print "    worst:           {}".format(elapsed_times[999])
    self.assertTrue(average_elapsed_time < 0.002) # should take 0.00087

    services.cleanup()

if __name__ == '__main__':
    unittest.main()
