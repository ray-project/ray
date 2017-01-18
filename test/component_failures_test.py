from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import sys
import time
import unittest

class ComponentFailureTest(unittest.TestCase):
  # This test checks that when a worker dies in the middle of a get, the plasma
  # store and manager will not die.
  def testDyingWorkerGet(self):
    obj_id = 20 * b"a"
    @ray.remote
    def f():
      ray.worker.global_worker.plasma_client.get(obj_id)

    ray.init(num_workers=1, driver_mode=ray.SILENT_MODE)

    # Have the worker wait in a get call.
    f.remote()

    # Kill the worker.
    time.sleep(1)
    ray.services.all_processes[ray.services.PROCESS_TYPE_WORKER][0].terminate()
    time.sleep(0.1)

    # Seal the object so the store attempts to notify the worker that the get
    # has been fulfilled.
    ray.worker.global_worker.plasma_client.create(obj_id, 100)
    ray.worker.global_worker.plasma_client.seal(obj_id)
    time.sleep(0.1)

    # Make sure that nothing has died.
    self.assertTrue(ray.services.all_processes_alive(exclude=[ray.services.PROCESS_TYPE_WORKER]))
    ray.worker.cleanup()

  # This test checks that when a worker dies in the middle of a wait, the plasma
  # store and manager will not die.
  def testDyingWorkerWait(self):
    obj_id = 20 * b"a"
    @ray.remote
    def f():
      ray.worker.global_worker.plasma_client.wait([obj_id])

    ray.init(num_workers=1, driver_mode=ray.SILENT_MODE)

    # Have the worker wait in a get call.
    f.remote()

    # Kill the worker.
    time.sleep(1)
    ray.services.all_processes[ray.services.PROCESS_TYPE_WORKER][0].terminate()
    time.sleep(0.1)

    # Seal the object so the store attempts to notify the worker that the get
    # has been fulfilled.
    ray.worker.global_worker.plasma_client.create(obj_id, 100)
    ray.worker.global_worker.plasma_client.seal(obj_id)
    time.sleep(0.1)

    # Make sure that nothing has died.
    self.assertTrue(ray.services.all_processes_alive(exclude=[ray.services.PROCESS_TYPE_WORKER]))
    ray.worker.cleanup()

if __name__ == "__main__":
  unittest.main(verbosity=2)
