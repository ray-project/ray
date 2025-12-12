import asyncio
import math
from collections.abc import Callable
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from threading import Thread
from typing import List, Optional

import httpx
import pytest
from fastapi import FastAPI, Request
from starlette.responses import StreamingResponse

from ray import serve
from ray._common.test_utils import SignalActor, async_wait_for_condition
from ray.serve._private.test_utils import get_application_url
from ray.serve.batching import _RuntimeSummaryStatistics
from ray.serve.context import (
    _get_serve_batch_request_context,
    _get_serve_request_context,
)


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
    url = f"{get_application_url()}/?prompt={prompt_prefix}"
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

    url = f"{get_application_url()}/"

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
    serve_instance, n_requests: int, max_batch_size: int, max_concurrent_batches: int
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

    async with httpx.AsyncClient() as client:
        tasks1 = await send_k_requests(
            signal_actor,
            n_requests,
            min_num_batches,
            app_name="app_name",
            client=client,
        )
        prev_iter_times = await handle._get_curr_iteration_start_times.remote()
        await signal_actor.send.remote()  # unblock the batch handler now that we have the iter times

        assert len(prev_iter_times.start_times) >= min_num_batches
        assert len(await handle._get_handling_task_stack.remote()) is not None
        assert await handle._is_batching_task_alive.remote()

        tasks2 = await send_k_requests(
            signal_actor,
            n_requests,
            min_num_batches,
            app_name="app_name",
            client=client,
        )
        new_iter_times = await handle._get_curr_iteration_start_times.remote()
        await signal_actor.send.remote()  # unblock the batch handler now that we have the iter times

        assert len(new_iter_times.start_times) >= min_num_batches
        assert len(await handle._get_handling_task_stack.remote()) is not None
        assert await handle._is_batching_task_alive.remote()

        assert new_iter_times.min_start_time > prev_iter_times.max_start_time

        # Cancel and await all tasks to avoid "Task exception was never retrieved" warning.
        # We don't need the HTTP responses, just need to clean up the tasks properly.
        for task in tasks1 + tasks2:
            task.cancel()
        await asyncio.gather(*tasks1, *tasks2, return_exceptions=True)


async def send_k_requests(
    signal_actor: SignalActor,
    k: int,
    min_num_batches: float,
    app_name: str,
    client: httpx.AsyncClient,
) -> List[asyncio.Task]:
    """Send k requests and wait until at least min_num_batches are waiting.

    Returns the list of request tasks so they can be awaited by the caller
    after unblocking the batch handler.
    """
    await signal_actor.send.remote(True)  # type: ignore[attr-defined]
    tasks = []
    for _ in range(k):
        tasks.append(
            asyncio.create_task(
                client.get(f"{get_application_url(app_name=app_name)}/")
            )
        )
    await wait_for_n_waiters(
        signal_actor, lambda num_waiters: num_waiters >= min_num_batches
    )
    return tasks


async def wait_for_n_waiters(
    signal_actor: SignalActor, condition: Callable[[int], bool]
) -> None:
    async def poll() -> bool:
        num_waiters: int = await signal_actor.cur_num_waiters.remote()  # type: ignore[attr-defined]
        return condition(num_waiters)

    return await async_wait_for_condition(poll)


def test_batching_request_context(serve_instance):
    """Test that _get_serve_batch_request_context() works correctly with batching.

    With 6 requests and max_batch_size=3, Serve should create 2 batches processed in parallel.
    Each batch should have access to the request contexts of all requests in that batch,
    and context should be properly unset after processing.
    """

    @serve.deployment(max_ongoing_requests=10)
    class BatchContextTester:
        def __init__(self):
            self.batch_results = []

        @serve.batch(
            max_batch_size=3, batch_wait_timeout_s=1.0, max_concurrent_batches=2
        )
        async def handle_batch(self, batch):
            # Store results for verification
            batch_result = {
                "batch_size": len(batch),
                "batch_request_contexts": _get_serve_batch_request_context(),
                "current_request_context": _get_serve_request_context(),
            }
            self.batch_results.append(batch_result)

            return ["ok" for _ in range(len(batch))]

        async def __call__(self, request):
            return await self.handle_batch(1)

        async def get_results(self):
            return self.batch_results

    handle = serve.run(BatchContextTester.bind())

    def do_request():
        """Make a request with a specific request ID."""
        url = get_application_url()
        r = httpx.post(f"{url}/")
        r.raise_for_status()

    # Launch 6 requests. Expect 2 batches of 3 requests each.
    threads = [Thread(target=do_request) for _ in range(6)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Get results from the deployment
    batch_results = handle.get_results.remote().result()

    # Verify each batch has correct size and context
    total_requests_processed = 0
    request_ids_in_batch_context = set()

    for result in batch_results:
        # Batch context should contain all 3 request contexts
        assert (
            len(result["batch_request_contexts"]) == 3
        ), f"Expected 3 contexts in batch, got {result['batch_request_contexts']}"
        req_ids_in_batch_context = [
            ctx.request_id for ctx in result["batch_request_contexts"]
        ]
        assert (
            len(req_ids_in_batch_context) == 3
        ), f"Expected 3 batch request IDs, got {len(req_ids_in_batch_context)}"
        request_ids_in_batch_context.update(req_ids_in_batch_context)

        # Current request context read within the batcher should be a default empty context.
        current_request_context = result["current_request_context"]
        assert current_request_context.request_id == ""
        assert current_request_context.route == ""
        assert current_request_context.app_name == ""
        assert current_request_context.multiplexed_model_id == ""

        total_requests_processed += result["batch_size"]

    # Verify all 6 requests were processed
    assert (
        total_requests_processed == 6
    ), f"Expected 6 total requests processed, got {total_requests_processed}"
    assert (
        len(request_ids_in_batch_context) == 6
    ), f"Expected 6 unique request IDs, got {len(request_ids_in_batch_context)}"


def test_batch_size_fn_simple(serve_instance):
    """Test batch_size_fn with a simple custom batch size metric."""

    @serve.deployment
    class BatchSizeFnExample:
        def __init__(self):
            self.batches_received = []

        @serve.batch(
            max_batch_size=100,  # Set based on total size, not count
            batch_wait_timeout_s=0.5,
            batch_size_fn=lambda items: sum(item["size"] for item in items),
        )
        async def handle_batch(self, requests: List):
            # Record the batch for verification
            self.batches_received.append(requests)
            # Return results
            return [req["value"] * 2 for req in requests]

        async def __call__(self, request):
            return await self.handle_batch(request)

        def get_batches(self):
            return self.batches_received

    handle = serve.run(BatchSizeFnExample.bind())

    # Send requests with different sizes
    # Request 1: size=30, value=1
    # Request 2: size=40, value=2
    # Request 3: size=20, value=3
    # Request 4: size=25, value=4
    # Total of first 3 = 90 (< 100), but adding 4th would be 115 (> 100)
    requests = [
        {"size": 30, "value": 1},
        {"size": 40, "value": 2},
        {"size": 20, "value": 3},
        {"size": 25, "value": 4},
    ]

    result_futures = [handle.remote(req) for req in requests]
    results = [future.result() for future in result_futures]

    # Verify results are correct
    assert results == [2, 4, 6, 8]

    # Verify batching behavior
    batches = handle.get_batches.remote().result()
    # Should have created at least one batch
    assert len(batches) > 0


def test_batch_size_fn_graph_nodes(serve_instance):
    """Test batch_size_fn with a GNN-style use case (batching by total nodes)."""

    class Graph:
        def __init__(self, num_nodes: int, graph_id: int):
            self.num_nodes = num_nodes
            self.graph_id = graph_id

    @serve.deployment
    class GraphBatcher:
        def __init__(self):
            self.batch_sizes = []

        @serve.batch(
            max_batch_size=100,  # Max 100 nodes per batch
            batch_wait_timeout_s=0.5,
            batch_size_fn=lambda graphs: sum(g.num_nodes for g in graphs),
        )
        async def process_graphs(self, graphs: List[Graph]):
            # Record batch size (total nodes)
            total_nodes = sum(g.num_nodes for g in graphs)
            self.batch_sizes.append(total_nodes)
            # Return graph_id * num_nodes as result
            return [g.graph_id * g.num_nodes for g in graphs]

        async def __call__(self, graph):
            return await self.process_graphs(graph)

        def get_batch_sizes(self):
            return self.batch_sizes

    handle = serve.run(GraphBatcher.bind())

    # Create graphs with different node counts
    # Graph 1: 30 nodes, Graph 2: 40 nodes, Graph 3: 35 nodes, Graph 4: 50 nodes
    # First 3 total = 105 nodes (> 100), so should be 2 batches
    graphs = [
        Graph(num_nodes=30, graph_id=1),
        Graph(num_nodes=40, graph_id=2),
        Graph(num_nodes=35, graph_id=3),
        Graph(num_nodes=50, graph_id=4),
    ]

    result_futures = [handle.remote(g) for g in graphs]
    results = [future.result() for future in result_futures]

    # Verify results
    assert results == [30, 80, 105, 200]

    # Verify batch sizes respect the limit
    batch_sizes = handle.get_batch_sizes.remote().result()
    for batch_size in batch_sizes:
        # Each batch should have <= 100 nodes
        assert batch_size <= 100, f"Batch size {batch_size} exceeds limit of 100"


def test_batch_size_fn_token_count(serve_instance):
    """Test batch_size_fn with an NLP-style use case (batching by total tokens)."""

    @serve.deployment
    class TokenBatcher:
        @serve.batch(
            max_batch_size=1000,  # Max 1000 tokens per batch
            batch_wait_timeout_s=0.5,
            batch_size_fn=lambda sequences: sum(len(s.split()) for s in sequences),
        )
        async def process_sequences(self, sequences: List[str]):
            # Return word count for each sequence
            return [len(s.split()) for s in sequences]

        async def __call__(self, sequence):
            return await self.process_sequences(sequence)

    handle = serve.run(TokenBatcher.bind())

    # Create sequences with different token counts
    sequences = [
        "This is a short sequence",  # 5 tokens
        "This is a much longer sequence with many more words in it",  # 12 tokens
        "Short",  # 1 token
        "A B C D E F G H I J",  # 10 tokens
    ]

    result_futures = [handle.remote(s) for s in sequences]
    results = [future.result() for future in result_futures]

    # Verify results are correct
    assert results == [5, 12, 1, 10]


def test_batch_size_fn_validation():
    """Test that batch_size_fn validation works correctly."""
    from ray.serve.batching import batch

    # Test with non-callable batch_size_fn
    with pytest.raises(TypeError, match="batch_size_fn must be a callable or None"):

        @batch(batch_size_fn="not_a_function")
        async def my_batch_handler(items):
            return items


def test_batch_size_fn_default_behavior(serve_instance):
    """Test that default behavior (batch_size_fn=None) still works as expected."""

    @serve.deployment
    class DefaultBatcher:
        @serve.batch(max_batch_size=5, batch_wait_timeout_s=0.5)
        async def handle_batch(self, requests):
            return [r * 2 for r in requests]

        async def __call__(self, request):
            return await self.handle_batch(request)

    handle = serve.run(DefaultBatcher.bind())

    # Send 10 requests
    result_futures = [handle.remote(i) for i in range(10)]
    results = [future.result() for future in result_futures]

    # Verify all results are correct
    assert results == [i * 2 for i in range(10)]


def test_batch_size_fn_oversized_item_raises_error(serve_instance):
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class OversizedItemBatcher:
        @serve.batch(
            max_batch_size=10,
            batch_wait_timeout_s=0.5,
            batch_size_fn=lambda items: sum(item["size"] for item in items),
        )
        async def handle_batch(self, requests: List):
            return [req["value"] for req in requests]

        @app.post("/")
        async def f(self, request: Request):
            body = await request.json()
            return await self.handle_batch(body)

    serve.run(OversizedItemBatcher.bind())

    # Send a request with size > max_batch_size (15 > 10)
    # This should return a 500 error with RuntimeError message
    url = f"{get_application_url(use_localhost=True)}/"
    response = httpx.post(url, json={"size": 15, "value": "too_large"}, timeout=5)

    assert response.status_code == 500


def test_batch_size_fn_deferred_item_processed(serve_instance):
    @serve.deployment
    class DeferredItemBatcher:
        def __init__(self):
            self.batch_sizes = []

        @serve.batch(
            max_batch_size=10,
            batch_wait_timeout_s=0.5,
            batch_size_fn=lambda items: sum(item["size"] for item in items),
        )
        async def handle_batch(self, requests: List):
            # Record actual batch sizes for verification
            total_size = sum(req["size"] for req in requests)
            self.batch_sizes.append(total_size)
            return [req["value"] for req in requests]

        async def __call__(self, request):
            return await self.handle_batch(request)

        def get_batch_sizes(self):
            return self.batch_sizes

    handle = serve.run(DeferredItemBatcher.bind())

    # Send requests where some will need to be deferred:
    # Request 1: size=6 (fits)
    # Request 2: size=6 (would make total 12 > 10, deferred)
    # Request 3: size=3 (fits with request 1, total 9)
    # Request 4: size=4 (would make total 13 > 10, deferred)
    requests = [
        {"size": 6, "value": "a"},
        {"size": 6, "value": "b"},
        {"size": 3, "value": "c"},
        {"size": 4, "value": "d"},
    ]

    result_futures = [handle.remote(req) for req in requests]
    results = [future.result() for future in result_futures]

    # All requests should be processed successfully
    assert set(results) == {"a", "b", "c", "d"}

    # Verify total size processed equals sum of all request sizes
    batch_sizes = handle.get_batch_sizes.remote().result()
    total_processed = sum(batch_sizes)
    expected_total = sum(req["size"] for req in requests)  # 6 + 6 + 3 + 4 = 19
    assert (
        total_processed == expected_total
    ), f"Total processed {total_processed} != expected {expected_total}"


def test_batch_size_fn_mixed_normal_and_large_items(serve_instance):
    @serve.deployment
    class MixedSizeBatcher:
        def __init__(self):
            self.batches_processed = []

        @serve.batch(
            max_batch_size=100,
            batch_wait_timeout_s=0.5,
            batch_size_fn=lambda items: sum(item["tokens"] for item in items),
        )
        async def handle_batch(self, requests: List):
            batch_info = {
                "total_tokens": sum(req["tokens"] for req in requests),
                "num_items": len(requests),
            }
            self.batches_processed.append(batch_info)
            return [f"processed_{req['id']}" for req in requests]

        async def __call__(self, request):
            return await self.handle_batch(request)

        def get_batches(self):
            return self.batches_processed

    handle = serve.run(MixedSizeBatcher.bind())

    # Mix of small and larger items
    requests = [
        {"id": 1, "tokens": 10},  # Small
        {"id": 2, "tokens": 20},  # Small
        {"id": 3, "tokens": 50},  # Medium
        {"id": 4, "tokens": 15},  # Small
        {"id": 5, "tokens": 90},  # Large (near limit)
        {"id": 6, "tokens": 5},  # Small
    ]

    result_futures = [handle.remote(req) for req in requests]
    results = [future.result() for future in result_futures]

    # All requests should be processed
    expected_results = [f"processed_{i}" for i in range(1, 7)]
    assert set(results) == set(expected_results)

    # Verify total tokens processed equals sum of all request tokens
    batches = handle.get_batches.remote().result()
    total_tokens_processed = sum(batch["total_tokens"] for batch in batches)
    expected_total = sum(req["tokens"] for req in requests)  # 10+20+50+15+90+5 = 190
    assert (
        total_tokens_processed == expected_total
    ), f"Total tokens {total_tokens_processed} != expected {expected_total}"

    # Verify total items processed equals number of requests
    total_items = sum(batch["num_items"] for batch in batches)
    assert total_items == len(
        requests
    ), f"Total items {total_items} != expected {len(requests)}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
