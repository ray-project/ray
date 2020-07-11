import unittest
from unittest.mock import patch

import ray
from ray.rllib.utils.actors import TaskPool


def createMockWorkerAndObjectRef(obj_ref):
    return ({obj_ref: 1}, obj_ref)


class TaskPoolTest(unittest.TestCase):
    @patch("ray.wait")
    def test_completed_prefetch_yieldsAllComplete(self, rayWaitMock):
        task1 = createMockWorkerAndObjectRef(1)
        task2 = createMockWorkerAndObjectRef(2)
        # Return the second task as complete and the first as pending
        rayWaitMock.return_value = ([2], [1])

        pool = TaskPool()
        pool.add(*task1)
        pool.add(*task2)

        fetched = list(pool.completed_prefetch())
        self.assertListEqual(fetched, [task2])

    @patch("ray.wait")
    def test_completed_prefetch_yieldsAllCompleteUpToDefaultLimit(
            self, rayWaitMock):
        # Load the pool with 1000 tasks, mock them all as complete and then
        # check that the first call to completed_prefetch only yields 999
        # items and the second call yields the final one
        pool = TaskPool()
        for i in range(1000):
            task = createMockWorkerAndObjectRef(i)
            pool.add(*task)

        rayWaitMock.return_value = (list(range(1000)), [])

        # For this test, we're only checking the object refs
        fetched = [pair[1] for pair in pool.completed_prefetch()]
        self.assertListEqual(fetched, list(range(999)))

        # Finally, check the next iteration returns the final taks
        fetched = [pair[1] for pair in pool.completed_prefetch()]
        self.assertListEqual(fetched, [999])

    @patch("ray.wait")
    def test_completed_prefetch_yieldsAllCompleteUpToSpecifiedLimit(
            self, rayWaitMock):
        # Load the pool with 1000 tasks, mock them all as complete and then
        # check that the first call to completed_prefetch only yield 999 items
        # and the second call yields the final one
        pool = TaskPool()
        for i in range(1000):
            task = createMockWorkerAndObjectRef(i)
            pool.add(*task)

        rayWaitMock.return_value = (list(range(1000)), [])

        # Verify that only the first 500 tasks are returned, this should leave
        # some tasks in the _fetching deque for later
        fetched = [pair[1] for pair in pool.completed_prefetch(max_yield=500)]
        self.assertListEqual(fetched, list(range(500)))

        # Finally, check the next iteration returns the remaining tasks
        fetched = [pair[1] for pair in pool.completed_prefetch()]
        self.assertListEqual(fetched, list(range(500, 1000)))

    @patch("ray.wait")
    def test_completed_prefetch_yieldsRemainingIfIterationStops(
            self, rayWaitMock):
        # Test for issue #7106
        # In versions of Ray up to 0.8.1, if the pre-fetch generator failed to
        # run to completion, then the TaskPool would fail to clear up already
        # fetched tasks resulting in stale object refs being returned
        pool = TaskPool()
        for i in range(10):
            task = createMockWorkerAndObjectRef(i)
            pool.add(*task)

        rayWaitMock.return_value = (list(range(10)), [])

        # This should fetch just the first item in the list
        try:
            for _ in pool.completed_prefetch():
                # Simulate a worker failure returned by ray.get()
                raise ray.exceptions.RayError
        except ray.exceptions.RayError:
            pass

        # This fetch should return the remaining pre-fetched tasks
        fetched = [pair[1] for pair in pool.completed_prefetch()]
        self.assertListEqual(fetched, list(range(1, 10)))

    @patch("ray.wait")
    def test_reset_workers_pendingFetchesFromFailedWorkersRemoved(
            self, rayWaitMock):
        pool = TaskPool()
        # We need to hold onto the tasks for this test so that we can fail a
        # specific worker
        tasks = []

        for i in range(10):
            task = createMockWorkerAndObjectRef(i)
            pool.add(*task)
            tasks.append(task)

        # Simulate only some of the work being complete and fetch a couple of
        # tasks in order to fill the fetching queue
        rayWaitMock.return_value = ([0, 1, 2, 3, 4, 5], [6, 7, 8, 9])
        fetched = [pair[1] for pair in pool.completed_prefetch(max_yield=2)]

        # As we still have some pending tasks, we need to update the
        # completion states to remove the completed tasks
        rayWaitMock.return_value = ([], [6, 7, 8, 9])

        pool.reset_workers([
            tasks[0][0],
            tasks[1][0],
            tasks[2][0],
            tasks[3][0],
            # OH NO! WORKER 4 HAS CRASHED!
            tasks[5][0],
            tasks[6][0],
            tasks[7][0],
            tasks[8][0],
            tasks[9][0]
        ])

        # Fetch the remaining tasks which should already be in the _fetching
        # queue
        fetched = [pair[1] for pair in pool.completed_prefetch()]
        self.assertListEqual(fetched, [2, 3, 5])


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
