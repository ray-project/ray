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
        manager = AsyncRequestsManager(workers, max_remote_requests_in_flight=1)
        for _ in range(4):
            manager.submit(lambda w: w.task())
        time.sleep(3)
        if not len(manager.get_ready_requests()) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have shorter tasks"
            )
        time.sleep(7)
        if not len(manager.get_ready_requests()) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have longer tasks"
            )

    def test_test_async_requests_task_buffering(self):
        """Tests that the async manager can properly buffer tasks and not
        schedule more inflight requests than allowed"""
        workers = [RemoteRLlibActor.remote(sleep_time=0.1) for _ in range(1)]
        manager = AsyncRequestsManager(workers, max_remote_requests_in_flight=2)
        for _ in range(4):
            manager.submit(lambda w: w.task())
        time.sleep(3)
        if not len(manager.get_ready_requests()[workers[0]]) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have shorter tasks"
            )
        time.sleep(3)
        if not len(manager.get_ready_requests()[workers[0]]) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have longer tasks"
            )

    def test_args_kwargs(self):
        """Tests that the async manager can properly handle actors with tasks that
        vary in the amount of time that they take to run"""
        workers = [RemoteRLlibActor.remote(sleep_time=0.1) for _ in range(1)]
        manager = AsyncRequestsManager(workers, max_remote_requests_in_flight=2)
        for _ in range(2):
            manager.submit(lambda w, a, b: w.task2(a, b), fn_args=[1, 2])
        time.sleep(3)
        if not len(manager.get_ready_requests()[workers[0]]) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have shorter tasks"
            )
        for _ in range(2):
            manager.submit(lambda w, a, b: w.task2(a, b), fn_kwargs=dict(a=1, b=2))
        time.sleep(3)
        if not len(manager.get_ready_requests()[workers[0]]) == 2:
            raise Exception(
                "We should return the 2 ready requests in this case from the actors"
                " that have longer tasks"
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
