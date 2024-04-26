import os
import pytest
import sys
import threading

import ray
from ray._private.test_utils import (
    wait_for_condition,
)
from ray.util.state import list_workers


_SYSTEM_CONFIG = {
    "task_events_report_interval_ms": 100,
    "metrics_report_interval_ms": 200,
    "enable_timeline": False,
    "gcs_mark_task_failed_on_job_done_delay_ms": 1000,
}


def test_worker_paused(shutdown_only):
    ray.init(num_cpus=1, _system_config=_SYSTEM_CONFIG)

    @ray.remote(max_retries=0)
    def f():
        import time

        # Pause 5 seconds inside debugger
        with ray._private.worker.global_worker.worker_paused_by_debugger():
            time.sleep(5)

    def verify(num_paused):
        workers = list_workers(
            filters=[("num_paused_threads", "=", num_paused)], detail=True
        )
        if len(workers) == 0:
            return False
        worker = workers[0]
        return worker.num_paused_threads == num_paused

    f_task = f.remote()  # noqa

    wait_for_condition(
        verify,
        timeout=20,
        retry_interval_ms=100,
        num_paused=1,
    )
    wait_for_condition(
        verify,
        timeout=20,
        retry_interval_ms=100,
        num_paused=0,
    )


@pytest.mark.parametrize("actor_concurrency", [1, 3])
def test_worker_paused_actor(shutdown_only, actor_concurrency):
    ray.init(_system_config=_SYSTEM_CONFIG)

    @ray.remote
    class TestActor:
        def main_task(self, i):
            if i == 0:
                import time

                # Pause 5 seconds inside debugger
                with ray._private.worker.global_worker.worker_paused_by_debugger():
                    time.sleep(5)

    def verify(num_paused):
        workers = list_workers(
            filters=[("num_paused_threads", "=", num_paused)], detail=True
        )
        if len(workers) == 0:
            return False
        worker = workers[0]
        return worker.num_paused_threads == num_paused

    test_actor = TestActor.options(max_concurrency=actor_concurrency).remote()
    refs = [  # noqa
        test_actor.main_task.options(name=f"TestActor.main_task_{i}").remote(i)
        for i in range(20)
    ]

    wait_for_condition(verify, num_paused=1)


@pytest.mark.parametrize("actor_concurrency", [1, 3])
def test_worker_paused_threaded_actor(shutdown_only, actor_concurrency):
    ray.init(_system_config=_SYSTEM_CONFIG)

    @ray.remote
    class ThreadedActor:
        def main_task(self):
            def thd_task():
                import time

                # Pause 5 seconds inside debugger
                with ray._private.worker.global_worker.worker_paused_by_debugger():  # noqa: E501
                    time.sleep(5)

            # create and start 10 threads
            thds = []
            for _ in range(10):
                thd = threading.Thread(target=thd_task)
                thd.start()
                thds.append(thd)

            # wait for all threads to finish
            for thd in thds:
                thd.join()

    def verify(num_paused):
        workers = list_workers(
            filters=[("num_paused_threads", "=", num_paused)], detail=True
        )
        if len(workers) == 0:
            return False
        worker = workers[0]
        return worker.num_paused_threads == num_paused

    threaded_actor = ThreadedActor.options(max_concurrency=actor_concurrency).remote()
    # this one call will create 10 threads and all of them will be paused
    threaded_actor.main_task.options(name="ThreadedActor.main_task").remote()
    wait_for_condition(verify, num_paused=10)

    # After waiting for 10 threads to finish, there should be no paused threads
    wait_for_condition(verify, num_paused=0)


@pytest.mark.parametrize("actor_concurrency", [1, 3])
def test_worker_paused_async_actor(shutdown_only, actor_concurrency):
    ray.init(_system_config=_SYSTEM_CONFIG)

    @ray.remote
    class AsyncActor:
        async def main_task(self):
            import time

            # Pause 5 seconds inside debugger
            with ray._private.worker.global_worker.worker_paused_by_debugger():
                time.sleep(5)

    def verify(num_paused):
        workers = list_workers(
            filters=[("num_paused_threads", "=", num_paused)], detail=True
        )
        if len(workers) == 0:
            return False
        worker = workers[0]
        return worker.num_paused_threads == num_paused

    async_actor = AsyncActor.options(max_concurrency=actor_concurrency).remote()
    refs = [async_actor.main_task.remote() for i in range(20)]  # noqa

    # This actor runs on a single thread, so num_paused should be 1
    wait_for_condition(verify, num_paused=1)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
