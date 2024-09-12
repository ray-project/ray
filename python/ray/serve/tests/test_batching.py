import asyncio
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from typing import List, Optional

import pytest
import requests
from starlette.responses import StreamingResponse

import ray
from ray import serve


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

    with pytest.raises(ray.exceptions.RayTaskError):
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
        futs = [pool.submit(partial(requests.get, url + str(idx))) for idx in range(4)]
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
        with pytest.raises(requests.exceptions.ReadTimeout):
            requests.get(url, timeout=0.005)

    # The following request should succeed.
    resp = requests.get(url, timeout=1)
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
        with pytest.raises(
            (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError)
        ):
            requests.get(url, timeout=0.005)

    # The following request should succeed.
    resp = requests.get(url, timeout=1)
    assert resp.status_code == 200
    assert resp.text == "0123456789"


def test_observability_helpers():
    """Checks observability helper methods that are used for batching.

    Tests three observability helper methods:
        * _get_curr_iteration_start_time: gets the current iteration's start
            time.
        * _is_batching_task_alive: returns whether the batch-handler task is
            alive.
        * _get_handling_task_stack: returns the stack for the batch-handler task.
    """

    @serve.deployment(name="batcher")
    class Batcher:
        @serve.batch(max_batch_size=3)
        async def handle_batch(self, requests):
            return [0] * len(requests)

        async def __call__(self, request):
            return await self.handle_batch(request)

        async def _get_curr_iteration_start_time(self) -> Optional[float]:
            return self.handle_batch._get_curr_iteration_start_time()

        async def _is_batching_task_alive(self) -> bool:
            return await self.handle_batch._is_batching_task_alive()

        async def _get_handling_task_stack(self) -> Optional[str]:
            return await self.handle_batch._get_handling_task_stack()

    serve.run(target=Batcher.bind(), name="app_name")
    handle = serve.get_deployment_handle(deployment_name="batcher", app_name="app_name")

    assert handle._is_batching_task_alive.remote().result()

    requests.get("http://localhost:8000/")

    assert len(handle._get_handling_task_stack.remote().result()) is not None
    assert handle._is_batching_task_alive.remote().result()

    curr_iteration_start_time = handle._get_curr_iteration_start_time.remote().result()

    for _ in range(5):
        requests.get("http://localhost:8000/")

    new_iteration_start_time = handle._get_curr_iteration_start_time.remote().result()

    assert new_iteration_start_time > curr_iteration_start_time
    assert len(handle._get_handling_task_stack.remote().result()) is not None
    assert handle._is_batching_task_alive.remote().result()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
