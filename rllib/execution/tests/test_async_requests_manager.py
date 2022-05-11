import random
import pytest
import unittest

import ray
import time

from ray.rllib.execution.parallel_requests import AsyncRequestsManager


@ray.remote
class RemoteRLlibActor:
    def __init__(self, sleep_time):
        self.sleep_time = sleep_time

    def apply(self, func, *_args, **_kwargs):
        return func(self, *_args, **_kwargs)

    def task(self):
        time.sleep(self.sleep_time)
        return "done"

    def task2(self, a, b):
        time.sleep(self.sleep_time)
        return a + b


class TestAsyncRequestsManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)
        random.seed(0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    @classmethod
    def shutdown_method(cls):
        ray.shutdown()

    def test_async_requests_manager_num_returns(self):
        """Tests that an async manager can properly handle actors with tasks that
        vary in the amount of time that they take to run"""
        workers = [RemoteRLlibActor.remote(sleep_time=0.1) for _ in range(2)]
        workers += [RemoteRLlibActor.remote(sleep_time=5) for _ in range(2)]
        manager = AsyncRequestsManager(
            workers, max_remote_requests_in_flight_per_worker=1
        )
        for _ in range(4):
            manager.submit(lambda w: w.task())
        time.sleep(3)
        if not len(manager.get_ready_results()) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have shorter tasks"
            )
        time.sleep(7)
        if not len(manager.get_ready_results()) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have longer tasks"
            )

    def test_test_async_requests_task_buffering(self):
        """Tests that the async manager can properly buffer tasks and not
        schedule more inflight requests than allowed"""
        workers = [RemoteRLlibActor.remote(sleep_time=0.1) for _ in range(2)]
        manager = AsyncRequestsManager(
            workers, max_remote_requests_in_flight_per_worker=2
        )
        for _ in range(8):
            manager.submit(lambda w: w.task())
        assert len(manager._pending_remotes) == 4
        time.sleep(3)
        ready_requests = manager.get_ready_results()
        for worker in workers:
            if not len(ready_requests[worker]) == 2:
                raise Exception(
                    "We should return the 2 ready requests in this case from each "
                    "actors"
                )
        # new tasks scheduled from the buffer
        time.sleep(3)
        ready_requests = manager.get_ready_results()
        for worker in workers:
            if not len(ready_requests[worker]) == 2:
                raise Exception(
                    "We should return the 2 ready requests in this case from each "
                    "actors"
                )

    def test_args_kwargs(self):
        """Tests that the async manager can properly handle actors with tasks that
        vary in the amount of time that they take to run"""
        workers = [RemoteRLlibActor.remote(sleep_time=0.1) for _ in range(1)]
        manager = AsyncRequestsManager(workers, max_remote_requests_in_flight=2)
        for _ in range(2):
            manager.submit(lambda w, a, b: w.task2(a, b), fn_args=[1, 2])
        time.sleep(3)
        if not len(manager.get_ready_results()[workers[0]]) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have shorter tasks"
            )
        for _ in range(2):
            manager.submit(lambda w, a, b: w.task2(a, b), fn_kwargs=dict(a=1, b=2))
        time.sleep(3)
        if not len(manager.get_ready_results()[workers[0]]) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have longer tasks"
            )

    def test_add_remove_actors(self):
        """Tests that the async manager can properly add and remove actors"""

        workers = []
        manager = AsyncRequestsManager(
            workers, max_remote_requests_in_flight_per_worker=2
        )
        if not (
            (
                len(manager._all_workers)
                == len(manager._remote_requests_in_flight)
                == len(manager._pending_to_actor)
                == len(manager._pending_remotes)
                == 0
            )
        ):
            raise ValueError("We should have no workers in this case.")
        with pytest.raises(ValueError, match="No workers.*"):
            manager.submit(lambda w: w.task())
        worker = RemoteRLlibActor.remote(sleep_time=0.1)
        manager.add_worker(worker)
        manager.submit(lambda w: w.task())
        if not (
            len(manager._remote_requests_in_flight[worker])
            == len(manager._pending_to_actor)
            == len(manager._all_workers)
            == len(manager._pending_remotes)
            == 1
            and manager._call_queue[worker].qsize() == 0
        ):
            raise ValueError("We should have 1 worker and 1 pending request")
        time.sleep(1)
        manager.get_ready_results()
        assert manager._call_queue[worker].qsize() == 0
        # test worker removal
        for i in range(2):
            manager.submit(lambda w: w.task())
            assert len(manager._pending_remotes) == i + 1
        manager.remove_worker(worker)
        if not (
            (
                len(manager._remote_requests_in_flight)
                == len(manager._pending_to_actor)
                == len(manager._all_workers)
                == len(manager._pending_remotes)
                == 0
            )
            and worker not in manager._call_queue
        ):
            raise ValueError(
                "We should have no workers and no queued/pending " "requests"
            )

    def test_submit_to_actor(self):
        workers = [RemoteRLlibActor.remote(sleep_time=0.1) for _ in range(2)]
        worker_not_in_manager = RemoteRLlibActor.remote(sleep_time=0.1)
        manager = AsyncRequestsManager(
            workers, max_remote_requests_in_flight_per_worker=2
        )
        manager.submit(lambda w: w.task(), actor=workers[0])
        time.sleep(3)
        results = manager.get_ready_results()
        if not len(results) == 1 and workers[0] not in results:
            raise Exception(
                "We should return the 1 ready requests in this case from the worker we "
                "submitted to"
            )
        with pytest.raises(ValueError, match=".*has not been added to the manager.*"):
            manager.submit(lambda w: w.task(), actor=worker_not_in_manager)

        with pytest.raises(
            ValueError,
            match=("Cannot specify actor and " "for_all_workers at the same"),
        ):
            manager.submit(lambda w: w.task(), actor=workers[0], for_all_workers=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
