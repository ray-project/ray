from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import ray
import time
import unittest

import pyarrow as pa


class ComponentFailureTest(unittest.TestCase):

    def tearDown(self):
        ray.worker.cleanup()

    # This test checks that when a worker dies in the middle of a get, the
    # plasma store and manager will not die.
    def testDyingWorkerGet(self):
        obj_id = 20 * b"a"

        @ray.remote
        def f():
            ray.worker.global_worker.plasma_client.get(obj_id)

        ray.worker._init(num_workers=1,
                         driver_mode=ray.SILENT_MODE,
                         start_workers_from_local_scheduler=False,
                         start_ray_local=True,
                         redirect_output=True)

        # Have the worker wait in a get call.
        f.remote()

        # Kill the worker.
        time.sleep(1)
        (ray.services
            .all_processes[ray.services.PROCESS_TYPE_WORKER][0].terminate())
        time.sleep(0.1)

        # Seal the object so the store attempts to notify the worker that the
        # get has been fulfilled.
        ray.worker.global_worker.plasma_client.create(
             pa.plasma.ObjectID(obj_id), 100)
        ray.worker.global_worker.plasma_client.seal(pa.plasma.ObjectID(obj_id))
        time.sleep(0.1)

        # Make sure that nothing has died.
        self.assertTrue(ray.services.all_processes_alive(
            exclude=[ray.services.PROCESS_TYPE_WORKER]))

    # This test checks that when a worker dies in the middle of a wait, the
    # plasma store and manager will not die.
    def testDyingWorkerWait(self):
        obj_id = 20 * b"a"

        @ray.remote
        def f():
            ray.worker.global_worker.plasma_client.wait([obj_id])

        ray.worker._init(num_workers=1,
                         driver_mode=ray.SILENT_MODE,
                         start_workers_from_local_scheduler=False,
                         start_ray_local=True,
                         redirect_output=True)

        # Have the worker wait in a get call.
        f.remote()

        # Kill the worker.
        time.sleep(1)
        (ray.services
            .all_processes[ray.services.PROCESS_TYPE_WORKER][0].terminate())
        time.sleep(0.1)

        # Seal the object so the store attempts to notify the worker that the
        # get has been fulfilled.
        ray.worker.global_worker.plasma_client.create(
            pa.plasma.ObjectID(obj_id), 100)
        ray.worker.global_worker.plasma_client.seal(pa.plasma.ObjectID(obj_id))
        time.sleep(0.1)

        # Make sure that nothing has died.
        self.assertTrue(ray.services.all_processes_alive(
            exclude=[ray.services.PROCESS_TYPE_WORKER]))

    def _testWorkerFailed(self, num_local_schedulers):
        @ray.remote
        def f(x):
            time.sleep(0.5)
            return x

        num_initial_workers = 4
        ray.worker._init(num_workers=(num_initial_workers *
                                      num_local_schedulers),
                         num_local_schedulers=num_local_schedulers,
                         start_workers_from_local_scheduler=False,
                         start_ray_local=True,
                         num_cpus=[num_initial_workers] * num_local_schedulers,
                         redirect_output=True)
        # Submit more tasks than there are workers so that all workers and
        # cores are utilized.
        object_ids = [f.remote(i) for i
                      in range(num_initial_workers * num_local_schedulers)]
        object_ids += [f.remote(object_id) for object_id in object_ids]
        # Allow the tasks some time to begin executing.
        time.sleep(0.1)
        # Kill the workers as the tasks execute.
        for worker in (ray.services
                          .all_processes[ray.services.PROCESS_TYPE_WORKER]):
            worker.terminate()
            time.sleep(0.1)
        # Make sure that we can still get the objects after the executing tasks
        # died.
        ray.get(object_ids)

    def testWorkerFailed(self):
        self._testWorkerFailed(1)

    def testWorkerFailedMultinode(self):
        self._testWorkerFailed(4)

    def _testComponentFailed(self, component_type):
        """Kill a component on all worker nodes and check workload succeeds."""
        @ray.remote
        def f(x, j):
            time.sleep(0.2)
            return x

        # Start with 4 workers and 4 cores.
        num_local_schedulers = 4
        num_workers_per_scheduler = 8
        ray.worker._init(
            num_workers=num_workers_per_scheduler,
            num_local_schedulers=num_local_schedulers,
            start_ray_local=True,
            num_cpus=[num_workers_per_scheduler] * num_local_schedulers,
            redirect_output=True)

        # Submit more tasks than there are workers so that all workers and
        # cores are utilized.
        object_ids = [f.remote(i, 0) for i
                      in range(num_workers_per_scheduler *
                               num_local_schedulers)]
        object_ids += [f.remote(object_id, 1) for object_id in object_ids]
        object_ids += [f.remote(object_id, 2) for object_id in object_ids]

        # Kill the component on all nodes except the head node as the tasks
        # execute.
        time.sleep(0.1)
        components = ray.services.all_processes[component_type]
        for process in components[1:]:
            process.terminate()
            time.sleep(1)

        for process in components[1:]:
            process.kill()
            process.wait()
            self.assertNotEqual(process.poll(), None)

        # Make sure that we can still get the objects after the executing tasks
        # died.
        results = ray.get(object_ids)
        expected_results = 4 * list(range(
            num_workers_per_scheduler * num_local_schedulers))
        self.assertEqual(results, expected_results)

    def check_components_alive(self, component_type, check_component_alive):
        """Check that a given component type is alive on all worker nodes.
        """
        components = ray.services.all_processes[component_type][1:]
        for component in components:
            if check_component_alive:
                self.assertTrue(component.poll() is None)
            else:
                print("waiting for " + component_type + " with PID " +
                      str(component.pid) + "to terminate")
                component.wait()
                print("done waiting for " + component_type + " with PID " +
                      str(component.pid) + "to terminate")
                self.assertTrue(not component.poll() is None)

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False),
        "Hanging with new GCS API.")
    def testLocalSchedulerFailed(self):
        # Kill all local schedulers on worker nodes.
        self._testComponentFailed(ray.services.PROCESS_TYPE_LOCAL_SCHEDULER)

        # The plasma stores and plasma managers should still be alive on the
        # worker nodes.
        self.check_components_alive(ray.services.PROCESS_TYPE_PLASMA_STORE,
                                    True)
        self.check_components_alive(ray.services.PROCESS_TYPE_PLASMA_MANAGER,
                                    True)
        self.check_components_alive(ray.services.PROCESS_TYPE_LOCAL_SCHEDULER,
                                    False)

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False),
        "Hanging with new GCS API.")
    def testPlasmaManagerFailed(self):
        # Kill all plasma managers on worker nodes.
        self._testComponentFailed(ray.services.PROCESS_TYPE_PLASMA_MANAGER)

        # The plasma stores should still be alive (but unreachable) on the
        # worker nodes.
        self.check_components_alive(ray.services.PROCESS_TYPE_PLASMA_STORE,
                                    True)
        self.check_components_alive(ray.services.PROCESS_TYPE_PLASMA_MANAGER,
                                    False)
        self.check_components_alive(ray.services.PROCESS_TYPE_LOCAL_SCHEDULER,
                                    False)

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False),
        "Hanging with new GCS API.")
    def testPlasmaStoreFailed(self):
        # Kill all plasma stores on worker nodes.
        self._testComponentFailed(ray.services.PROCESS_TYPE_PLASMA_STORE)

        # No processes should be left alive on the worker nodes.
        self.check_components_alive(ray.services.PROCESS_TYPE_PLASMA_STORE,
                                    False)
        self.check_components_alive(ray.services.PROCESS_TYPE_PLASMA_MANAGER,
                                    False)
        self.check_components_alive(ray.services.PROCESS_TYPE_LOCAL_SCHEDULER,
                                    False)

    def testDriverLivesSequential(self):
        ray.worker.init(redirect_output=True)
        all_processes = ray.services.all_processes
        processes = [
            all_processes[ray.services.PROCESS_TYPE_PLASMA_STORE][0],
            all_processes[ray.services.PROCESS_TYPE_PLASMA_MANAGER][0],
            all_processes[ray.services.PROCESS_TYPE_LOCAL_SCHEDULER][0],
            all_processes[ray.services.PROCESS_TYPE_GLOBAL_SCHEDULER][0]]

        # Kill all the components sequentially.
        for process in processes:
            process.terminate()
            time.sleep(0.1)
            process.kill()
            process.wait()

        # If the driver can reach the tearDown method, then it is still alive.

    def testDriverLivesParallel(self):
        ray.worker.init(redirect_output=True)
        all_processes = ray.services.all_processes
        processes = [
            all_processes[ray.services.PROCESS_TYPE_PLASMA_STORE][0],
            all_processes[ray.services.PROCESS_TYPE_PLASMA_MANAGER][0],
            all_processes[ray.services.PROCESS_TYPE_LOCAL_SCHEDULER][0],
            all_processes[ray.services.PROCESS_TYPE_GLOBAL_SCHEDULER][0]]

        # Kill all the components in parallel.
        for process in processes:
            process.terminate()

        time.sleep(0.1)
        for process in processes:
            process.kill()

        for process in processes:
            process.wait()

        # If the driver can reach the tearDown method, then it is still alive.


if __name__ == "__main__":
    unittest.main(verbosity=2)
