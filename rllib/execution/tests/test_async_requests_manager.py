import pytest
import random
import time
import unittest

import ray
from ray.rllib.execution.parallel_requests import AsyncRequestsManager


@ray.remote
class RemoteRLlibActor:
    def __init__(self, sleep_time):
        self.num_task_called = 0
        self.num_task2_called = 0
        self.sleep_time = sleep_time

    def apply(self, func, *_args, **_kwargs):
        return func(self, *_args, **_kwargs)

    def task(self):
        time.sleep(self.sleep_time)
        self.num_task_called += 1
        return "done"

    def task2(self, a, b):
        time.sleep(self.sleep_time)
        self.num_task2_called += 1
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
            manager.call(lambda w: w.task())
        time.sleep(3)
        if not len(manager.get_ready()) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have shorter tasks"
            )
        time.sleep(7)
        if not len(manager.get_ready()) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have longer tasks"
            )

    def test_round_robin_scheduling(self):
        """Test that the async manager schedules actors in a round robin fashion"""
        workers = [RemoteRLlibActor.remote(sleep_time=0.1) for _ in range(2)]
        manager = AsyncRequestsManager(
            workers, max_remote_requests_in_flight_per_worker=2
        )
        for i in range(4):
            scheduled_actor = workers[i % len(workers)]
            manager.call(lambda w: w.task())
            if i < 2:
                assert len(manager._remote_requests_in_flight[scheduled_actor]) == 1, (
                    "We should have 1 request in flight for the actor that we just "
                    "scheduled on"
                )
            else:
                assert len(manager._remote_requests_in_flight[scheduled_actor]) == 2, (
                    "We should have 2 request in flight for the actor that we just "
                    "scheduled on"
                )

    def test_test_async_requests_task_doesnt_buffering(self):
        """Tests that the async manager drops"""
        workers = [RemoteRLlibActor.remote(sleep_time=0.1) for _ in range(2)]
        manager = AsyncRequestsManager(
            workers, max_remote_requests_in_flight_per_worker=2
        )
        for i in range(8):
            scheduled = manager.call(lambda w: w.task())
            if i < 4:
                assert scheduled, "We should have scheduled the task"
            else:
                assert not scheduled, (
                    "We should not have scheduled the task because"
                    " all workers are busy."
                )
        assert len(manager._pending_remotes) == 4, "We should have 4 pending requests"
        time.sleep(3)
        ready_requests = manager.get_ready()
        for worker in workers:
            if not len(ready_requests[worker]) == 2:
                raise Exception(
                    "We should return the 2 ready requests in this case from each "
                    "actors."
                )
        for _ in range(4):
            manager.call(lambda w: w.task())
        # new tasks scheduled from the buffer
        time.sleep(3)
        ready_requests = manager.get_ready()
        for worker in workers:
            if not len(ready_requests[worker]) == 2:
                raise Exception(
                    "We should return the 2 ready requests in this case from each "
                    "actors"
                )

    def test_args_kwargs(self):
        """Tests that the async manager can properly handle actors with tasks that
        vary in the amount of time that they take to run"""
        workers = [RemoteRLlibActor.remote(sleep_time=0.1)]
        manager = AsyncRequestsManager(
            workers, max_remote_requests_in_flight_per_worker=2
        )
        for _ in range(2):
            manager.call(lambda w, a, b: w.task2(a, b), fn_args=[1, 2])
        time.sleep(3)
        if not len(manager.get_ready()[workers[0]]) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have shorter tasks"
            )
        for _ in range(2):
            manager.call(lambda w, a, b: w.task2(a, b), fn_kwargs=dict(a=1, b=2))
        time.sleep(3)
        if not len(manager.get_ready()[workers[0]]) == 2:
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

        assert not manager.call(lambda w: w.task()), (
            "Task shouldn't have been "
            "launched since there are no "
            "workers in the manager."
        )
        worker = RemoteRLlibActor.remote(sleep_time=0.1)
        manager.add_workers(worker)
        manager.call(lambda w: w.task())
        if not (
            len(manager._remote_requests_in_flight[worker])
            == len(manager._pending_to_actor)
            == len(manager._all_workers)
            == len(manager._pending_remotes)
            == 1
        ):
            raise ValueError("We should have 1 worker and 1 pending request")
        time.sleep(3)
        manager.get_ready()
        # test worker removal
        for i in range(2):
            manager.call(lambda w: w.task())
            assert len(manager._pending_remotes) == i + 1
        manager.remove_workers(worker)
        if not ((len(manager._all_workers) == 0)):
            raise ValueError("We should have no workers that we can schedule tasks to")
        if not (
            (len(manager._pending_remotes) == 2 and len(manager._pending_to_actor) == 2)
        ):
            raise ValueError(
                "We should still have 2 pending requests in flight from the worker"
            )
        time.sleep(3)
        result = manager.get_ready()
        if not (
            len(result) == 1
            and len(result[worker]) == 2
            and len(manager._pending_remotes) == 0
            and len(manager._pending_to_actor) == 0
        ):
            raise ValueError(
                "We should have 2 ready results from the worker and no pending requests"
            )

    def test_call_to_actor(self):
        workers = [RemoteRLlibActor.remote(sleep_time=0.1) for _ in range(2)]
        worker_not_in_manager = RemoteRLlibActor.remote(sleep_time=0.1)
        manager = AsyncRequestsManager(
            workers, max_remote_requests_in_flight_per_worker=2
        )
        manager.call(lambda w: w.task(), actor=workers[0])
        time.sleep(3)
        results = manager.get_ready()
        if not len(results) == 1 and workers[0] not in results:
            raise Exception(
                "We should return the 1 ready requests in this case from the worker we "
                "called to"
            )
        with pytest.raises(ValueError, match=".*has not been added to the manager.*"):
            manager.call(lambda w: w.task(), actor=worker_not_in_manager)

    def test_high_load(self):
        workers = [
            RemoteRLlibActor.remote(sleep_time=random.random() * 2.0) for _ in range(60)
        ]
        manager = AsyncRequestsManager(
            workers,
            max_remote_requests_in_flight_per_worker=2,
            return_object_refs=True,
            ray_wait_timeout_s=0.0,
        )
        num_ready = 0
        for i in range(2000):
            manager.call_on_all_available(lambda w: w.task())
            time.sleep(0.01)

            ready = manager.get_ready()

            for reqs in ready.values():
                num_ready += len(reqs)
                ray.get(reqs)

            for worker in ready.keys():
                worker.task2.remote(1, 3)

        time.sleep(20)

        ready = manager.get_ready()
        num_ready += sum(len(reqs) for reqs in ready.values())

        actually_called = sum(
            ray.get(
                [worker.apply.remote(lambda w: w.num_task_called) for worker in workers]
            )
        )
        assert actually_called == num_ready, (actually_called, num_ready)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
