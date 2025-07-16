import asyncio
import math
from collections.abc import Callable
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from typing import List, Optional

import httpx
import pytest
from starlette.responses import StreamingResponse

from ray import serve
from ray._common.test_utils import SignalActor, async_wait_for_condition
from ray.serve.batching import _RuntimeSummaryStatistics


def test_batching(serve_instance):
    @serve.deployment
    class BatchingExample:
        def __init__(self):
            self.count = 0

        @serve.batch(max_batch_size=5, batch_wait_timeout_s=1)
        async def handle_batch(self, requests):
            self.count += 1
            batch_size = len(requests)
            return [self.count] * batch_size

        async def __call__(self, request):
            return await self.handle_batch(request)

    handle = serve.run(BatchingExample.bind())

    result_list = [handle.remote(1) for _ in range(20)]
    # since count is only updated per batch of queries
    # If there atleast one __call__ fn call with batch size greater than 1
    # counter result will always be less than 20
    assert max([r.result() for r in result_list]) < 20


def test_concurrent_batching(serve_instance):
    BATCHES_IN_FLIGHT = 2
    MAX_BATCH_SIZE = 5
    BATCH_WAIT_TIMEOUT_S = 1
    MAX_REQUESTS_IN_FLIGHT = BATCHES_IN_FLIGHT * MAX_BATCH_SIZE

    @serve.deployment(max_ongoing_requests=MAX_REQUESTS_IN_FLIGHT * 2)
    class BatchingExample:
        def __init__(self):
            self.n_batches_in_flight = 0
            self.n_requests_in_flight = 0

        @serve.batch(
            max_batch_size=MAX_BATCH_SIZE,
            batch_wait_timeout_s=BATCH_WAIT_TIMEOUT_S,
            max_concurrent_batches=BATCHES_IN_FLIGHT,
        )
        async def handle_batch(self, requests):
            self.n_batches_in_flight += 1
            self.n_requests_in_flight += len(requests)
            await asyncio.sleep(0.5)
            out = [
                (req_idx, self.n_batches_in_flight, self.n_requests_in_flight)
                for req_idx in requests
            ]
            await asyncio.sleep(0.5)
            self.n_requests_in_flight -= len(requests)
            self.n_batches_in_flight -= 1
            return out

        async def __call__(self, request):
            return await self.handle_batch(request)

    handle = serve.run(BatchingExample.bind())

    idxs = set(range(20))
    result_futures = [handle.remote(i) for i in idxs]
    result_list = [future.result() for future in result_futures]

    out_idxs = set()
    for idx, batches_in_flight, requests_in_flight in result_list:
        out_idxs.add(idx)
        assert (
            batches_in_flight == BATCHES_IN_FLIGHT
        ), f"Should have been {BATCHES_IN_FLIGHT} batches in flight at all times, got {batches_in_flight}"
        assert (
            requests_in_flight == MAX_REQUESTS_IN_FLIGHT
        ), f"Should have been {MAX_REQUESTS_IN_FLIGHT} requests in flight at all times, got {requests_in_flight}"

    assert idxs == out_idxs, "All requests should be processed"


def test_batching_exception(serve_instance):
    @serve.deployment
    class NoListReturned:
        def __init__(self):
            self.count = 0

        @serve.batch(max_batch_size=5)
        async def handle_batch(self, requests):
            return len(requests)

        async def __call__(self, request):
            return await self.handle_batch(request)

    # Set the max batch size.
    handle = serve.run(NoListReturned.bind())

    with pytest.raises(TypeError):
        assert handle.remote(1).result()


@pytest.mark.asyncio
async def test_batch_generator_streaming_response_integration_test(serve_instance):
    NUM_YIELDS = 10

    @serve.deployment
    class Textgen:
        @serve.batch(max_batch_size=4, batch_wait_timeout_s=1000)
        async def batch_handler(self, prompts: List[str]):
            for _ in range(NUM_YIELDS):
                # Check that the batch handler can yield unhashable types
                prompt_responses = [{"value": prompt} for prompt in prompts]
                yield prompt_responses

        async def value_extractor(self, prompt_responses):
            async for prompt_response in prompt_responses:
                yield prompt_response["value"]

        async def __call__(self, request):
            prompt = request.query_params["prompt"]
            response_values = self.value_extractor(self.batch_handler(prompt))
            return StreamingResponse(response_values)

    serve.run(Textgen.bind())

    prompt_prefix = "hola"
    url = f"http://localhost:8000/?prompt={prompt_prefix}"
    with ThreadPoolExecutor() as pool:
        futs = [pool.submit(partial(httpx.get, url + str(idx))) for idx in range(4)]
        responses = [fut.result() for fut in futs]

    for idx, response in enumerate(responses):
        assert response.status_code == 200
        assert response.text == "".join([prompt_prefix + str(idx)] * NUM_YIELDS)


def test_batching_client_dropped_unary(serve_instance):
    """Test unary batching with clients that drops the connection.

    After requests are dropped. The next request should succeed.
    """

    @serve.deployment
    class ModelUnary:
        @serve.batch(max_batch_size=5)
        async def handle_batch(self, requests):
            await asyncio.sleep(0.05)
            return ["fake-response" for _ in range(len(requests))]

        async def __call__(self, request):
            return await self.handle_batch(request)

    serve.run(ModelUnary.bind())

    url = "http://localhost:8000/"

    # Sending requests with clients that drops the connection.
    for _ in range(3):
        with pytest.raises(httpx.ReadTimeout):
            httpx.get(url, timeout=0.005)

    # The following request should succeed.
    resp = httpx.get(url, timeout=1)
    assert resp.status_code == 200
    assert resp.text == "fake-response"


def test_batching_client_dropped_streaming(serve_instance):
    """Test streaming batching with clients that drops the connection.

    After requests are dropped. The next request should succeed.
    """

    @serve.deployment
    class ModelStreaming:
        @serve.batch(max_batch_size=3)
        async def handle_batch(self, requests):
            await asyncio.sleep(0.05)
            for i in range(10):
                yield [str(i) for _ in range(len(requests))]

        async def __call__(self, request):
            return StreamingResponse(self.handle_batch(request))

    serve.run(ModelStreaming.bind())

    url = "http://localhost:8000/"

    # Sending requests with clients that drops the connection.
    for _ in range(3):
        with pytest.raises((httpx.ReadTimeout, httpx.ConnectError)):
            httpx.get(url, timeout=0.005)

    # The following request should succeed.
    resp = httpx.get(url, timeout=1)
    assert resp.status_code == 200
    assert resp.text == "0123456789"


@pytest.mark.asyncio
@pytest.mark.parametrize("max_concurrent_batches", [1, 10])
@pytest.mark.parametrize("max_batch_size", [1, 10])
@pytest.mark.parametrize("n_requests", [1, 10])
async def test_observability_helpers(
    n_requests: int, max_batch_size: int, max_concurrent_batches: int
) -> None:
    """Checks observability helper methods that are used for batching.

    Tests three observability helper methods:
        * _get_curr_iteration_start_times: gets the current iteration's start
            time.
        * _is_batching_task_alive: returns whether the batch-handler task is
            alive.
        * _get_handling_task_stack: returns the stack for the batch-handler task.
    """

    signal_actor = SignalActor.remote()

    @serve.deployment(
        name="batcher", max_ongoing_requests=max_concurrent_batches * max_batch_size
    )
    class Batcher:
        @serve.batch(
            max_batch_size=max_batch_size,
            max_concurrent_batches=max_concurrent_batches,
            batch_wait_timeout_s=0.1,
        )
        async def handle_batch(self, requests):
            await signal_actor.wait.remote()  # wait until the outer signal actor is released
            return [0] * len(requests)

        async def __call__(self, request):
            return await self.handle_batch(request)

        async def _get_curr_iteration_start_times(self) -> _RuntimeSummaryStatistics:
            return self.handle_batch._get_curr_iteration_start_times()

        async def _is_batching_task_alive(self) -> bool:
            return await self.handle_batch._is_batching_task_alive()

        async def _get_handling_task_stack(self) -> Optional[str]:
            return await self.handle_batch._get_handling_task_stack()

    serve.run(target=Batcher.bind(), name="app_name")
    handle = serve.get_deployment_handle(deployment_name="batcher", app_name="app_name")

    assert await handle._is_batching_task_alive.remote()

    min_num_batches = min(
        math.ceil(n_requests / max_batch_size), max_concurrent_batches
    )

    await send_k_requests(signal_actor, n_requests, min_num_batches)
    prev_iter_times = await handle._get_curr_iteration_start_times.remote()
    await signal_actor.send.remote()  # unblock the batch handler now that we have the iter times

    assert len(prev_iter_times.start_times) >= min_num_batches
    assert len(await handle._get_handling_task_stack.remote()) is not None
    assert await handle._is_batching_task_alive.remote()

    await send_k_requests(signal_actor, n_requests, min_num_batches)
    new_iter_times = await handle._get_curr_iteration_start_times.remote()
    await signal_actor.send.remote()  # unblock the batch handler now that we have the iter times

    assert len(new_iter_times.start_times) >= min_num_batches
    assert len(await handle._get_handling_task_stack.remote()) is not None
    assert await handle._is_batching_task_alive.remote()

    assert new_iter_times.min_start_time > prev_iter_times.max_start_time


async def send_k_requests(
    signal_actor: SignalActor, k: int, min_num_batches: float
) -> None:
    """Send k requests and wait until at least min_num_batches are waiting."""
    await signal_actor.send.remote(True)  # type: ignore[attr-defined]
    async with httpx.AsyncClient() as client:
        for _ in range(k):
            asyncio.create_task(client.get("http://localhost:8000/"))
        await wait_for_n_waiters(
            signal_actor, lambda num_waiters: num_waiters >= min_num_batches
        )


async def wait_for_n_waiters(
    signal_actor: SignalActor, condition: Callable[[int], bool]
) -> None:
    async def poll() -> bool:
        num_waiters: int = await signal_actor.cur_num_waiters.remote()  # type: ignore[attr-defined]
        return condition(num_waiters)

    return await async_wait_for_condition(poll)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
