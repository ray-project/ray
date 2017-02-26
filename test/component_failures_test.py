from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import sys
import time
import unittest
import os

class ComponentFailureTest(unittest.TestCase):
  # This test checks that when a worker dies in the middle of a get, the plasma
  # store and manager will not die.
  def testDyingWorkerGet(self):
    obj_id = 20 * b"a"
    @ray.remote
    def f():
      ray.worker.global_worker.plasma_client.get(obj_id)

    ray.worker._init(num_workers=1,
                     driver_mode=ray.SILENT_MODE,
                     start_workers_from_local_scheduler=False,
                     start_ray_local=True)

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

    ray.worker._init(num_workers=1,
                     driver_mode=ray.SILENT_MODE,
                     start_workers_from_local_scheduler=False,
                     start_ray_local=True)

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

  def _testWorkerFailed(self, num_local_schedulers):
    @ray.remote
    def f(x):
      time.sleep(0.5)
      return x

    num_initial_workers = 4
    ray.worker._init(num_workers=num_initial_workers * num_local_schedulers,
                     num_local_schedulers=num_local_schedulers,
                     start_workers_from_local_scheduler=False,
                     start_ray_local=True,
                     num_cpus=[num_initial_workers] * num_local_schedulers)
    # Submit more tasks than there are workers so that all workers and cores
    # are utilized.
    object_ids = [f.remote(i) for i in range(num_initial_workers * num_local_schedulers)]
    object_ids += [f.remote(object_id) for object_id in object_ids]
    # Allow the tasks some time to begin executing.
    time.sleep(0.1)
    # Kill the workers as the tasks execute.
    for worker in ray.services.all_processes[ray.services.PROCESS_TYPE_WORKER]:
      worker.terminate()
      time.sleep(0.1)
    # Make sure that we can still get the objects after the executing tasks died.
    ray.get(object_ids)

    ray.worker.cleanup()

  def testWorkerFailed(self):
    self._testWorkerFailed(1)

  def testWorkerFailedMultinode(self):
    self._testWorkerFailed(4)

if __name__ == "__main__":
  unittest.main(verbosity=2)
