import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayTaskError, TaskCancelledError
from ray.util.state import list_workers


@ray.remote(num_cpus=1)
class EndpointActor:
    def __init__(self, *, injected_executor_delay_s: float, tokens_per_request: int):
        self._tokens_per_request = tokens_per_request
        # In this test we simulate conditions leading to use-after-free conditions,
        # by injecting delays into worker's thread-pool executor
        self._inject_delay_in_core_worker_executor(
            target_delay_s=injected_executor_delay_s,
            max_workers=1,
        )

    async def aio_stream(self):
        for i in range(self._tokens_per_request):
            yield i

    @classmethod
    def _inject_delay_in_core_worker_executor(
        cls, target_delay_s: float, max_workers: int
    ):
        if target_delay_s > 0:

            class DelayedThreadPoolExecutor(ThreadPoolExecutor):
                def submit(self, fn, /, *args, **kwargs):
                    def __slowed_fn():
                        print(
                            f">>> [DelayedThreadPoolExecutor] Starting executing "
                            f"function with delay {target_delay_s}s"
                        )

                        time.sleep(target_delay_s)
                        fn(*args, **kwargs)

                    return super().submit(__slowed_fn)

            executor = DelayedThreadPoolExecutor(max_workers=max_workers)
            ray._private.worker.global_worker.core_worker.reset_event_loop_executor(
                executor
            )


@ray.remote(num_cpus=1)
class CallerActor:
    def __init__(
        self,
        downstream: ActorHandle,
    ):
        self._h = downstream

    async def run(self):
        print(">>> [Caller] Starting consuming stream")

        async_obj_ref_gen = self._h.aio_stream.options(num_returns="streaming").remote()
        async for ref in async_obj_ref_gen:
            r = await ref
            if r == 1:
                print(">>> [Caller] Cancelling generator")
                ray.cancel(async_obj_ref_gen, recursive=False)

                # NOTE: This delay is crucial to let already scheduled task to report
                #       generated item (report_streaming_generator_output) before we
                #       will tear down this stream
                delay_after_cancellation_s = 2

                print(f">>> [Caller] **Sleeping** {delay_after_cancellation_s}s")
                time.sleep(delay_after_cancellation_s)
            else:
                print(f">>> [Caller] Received {r}")

        print(">>> [Caller] Completed consuming stream")


@pytest.mark.parametrize("injected_executor_delay_s", [0, 2])
@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_nodes": 2,
            "num_cpus": 1,
        }
    ],
    indirect=True,
)
def test_segfault_report_streaming_generator_output(
    ray_start_cluster, injected_executor_delay_s: float
):
    """
    This is a "smoke" test attempting to emulate condition, when using Ray's async
    streaming generator, that leads to worker crashing with SIGSEGV.

    For more details summarizing these conditions, please refer to
    https://github.com/ray-project/ray/issues/43771#issuecomment-1982301654
    """

    caller = CallerActor.remote(
        EndpointActor.remote(
            injected_executor_delay_s=injected_executor_delay_s,
            tokens_per_request=100,
        ),
    )

    worker_state_before = [(a.worker_id, a.exit_type) for a in list_workers()]
    print(">>> Workers state before: ", worker_state_before)

    try:
        ray.get(caller.run.remote())
    except Exception as exc:
        # There is a small chance that the task cancellation signal will arrive
        # late at the executor, after the task has already finished. In that
        # case, the task will complete normally, with no exception thrown.
        # Thus, we wrap ray.get in a try-catch instead of asserting an
        # exception.
        assert isinstance(exc, RayTaskError)
        assert isinstance(exc.cause, TaskCancelledError)

    worker_state_after = [(a.worker_id, a.exit_type) for a in list_workers()]
    print(">>> Workers state after: ", worker_state_after)

    worker_ids, worker_exit_types = zip(*worker_state_after)
    # Make sure no workers crashed
    assert (
        "SYSTEM_ERROR" not in worker_exit_types
    ), f"Unexpected crashed worker(s) in {worker_ids}"


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
