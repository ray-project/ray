import unittest
from unittest.mock import Mock, patch

import ray
from ray.rllib.utils.actors import TaskPool

def createMockWorkerAndObjectId(obj_id):
    return ({
        obj_id: 1
    }, obj_id)


class TaskPoolTest(unittest.TestCase):
    @patch("ray.wait")
    def test_completed_prefetch_yieldsAllComplete(self, rayWaitMock):
        task1 = createMockWorkerAndObjectId(1)
        task2 = createMockWorkerAndObjectId(2)
        # Return the second task as complete and the first as pending
        rayWaitMock.return_value = ([2], [1])

        pool = TaskPool()
        pool.add(*task1)
        pool.add(*task2)

        fetched = [pair for pair in pool.completed_prefetch()]
        self.assertListEqual(fetched, [task2])

    @patch("ray.wait")
    def test_completed_prefetch_yieldsAllCompleteUpToDefaultLimit(self, rayWaitMock):
        # Load the pool with 1000 tasks, mock them all as complete and then check
        # that the first call to completed_prefetch only yield 999 items and the
        # second call yields the final one
        pool = TaskPool()
        for i in range(1000):
            task = createMockWorkerAndObjectId(i)
            pool.add(*task)

        rayWaitMock.return_value = ([i for i in range(1000)], [])

        # For this test, we're only checking the object ids
        fetched = [pair[1] for pair in pool.completed_prefetch()]
        self.assertListEqual(fetched, [i for i in range(999)])

        # Finally, check the next iteration returns the final taks
        fetched = [pair[1] for pair in pool.completed_prefetch()]
        self.assertListEqual(fetched, [999])

    @patch("ray.wait")
    def test_completed_prefetch_yieldsAllCompleteUpToSpecifiedLimit(self, rayWaitMock):
        # Load the pool with 1000 tasks, mock them all as complete and then check
        # that the first call to completed_prefetch only yield 999 items and the
        # second call yields the final one
        pool = TaskPool()
        for i in range(1000):
            task = createMockWorkerAndObjectId(i)
            pool.add(*task)

        rayWaitMock.return_value = ([i for i in range(1000)], [])

        # For this test, we're only checking the object ids
        fetched = [pair[1] for pair in pool.completed_prefetch(max_yield=500)]
        self.assertListEqual(fetched, [i for i in range(500)])

        # Finally, check the next iteration returns the final taks
        fetched = [pair[1] for pair in pool.completed_prefetch()]
        self.assertListEqual(fetched, [i for i in range(500, 1000)])

    @patch("ray.wait")
    def test_completed_prefetch_yieldsRemainingIfIterationStops(self, rayWaitMock):
        # Test for issue #7106
        # In versions of Ray up to 0.8.1, if the pre-fetch generator failed to run to
        # completion, then the TaskPool would fail to clear up already fetched tasks
        # resulting in stale object ids being returned
        pool = TaskPool()
        for i in range(10):
            task = createMockWorkerAndObjectId(i)
            pool.add(*task)
        
        rayWaitMock.return_value = ([i for i in range(10)], [])

        # This should fetch just the first item in the list
        try:
            for _ in pool.completed_prefetch():
                # Simulate a worker failure returned by ray.get()
                raise ray.exceptions.RayError
        except ray.exceptions.RayError:
            pass

        # This fetch should return the remaining pre-fetched tasks
        fetched = [pair[1] for pair in pool.completed_prefetch()]
        self.assertListEqual(fetched, [i for i in range(1, 10)])


if __name__ == "__main__":
    unittest.main(verbosity=2)
