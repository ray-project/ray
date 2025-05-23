from typing import List

import asyncio
import time
from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment(max_ongoing_requests=(8 * 3))
class Model:
    @serve.batch(max_batch_size=8, batch_wait_timeout_s=5, max_concurrent_batches=2)
    async def __call__(self, multiple_samples: List[int]) -> List[int]:
        print(f"Processing batch of size {len(multiple_samples)} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        await asyncio.sleep(1)
        for i in multiple_samples:
            yield i * 2


async def main1():
    handle: DeploymentHandle = serve.run(Model.bind())
    remote_fn = handle.options(stream=True)
    responses = await asyncio.gather(*map(remote_fn.remote, range(8 * 3)))
    for response in responses:
        print(response)


def test_concurrent_batching():
    BATCHES_IN_FLIGHT = 2
    MAX_BATCH_SIZE = 5
    BATCH_WAIT_TIMEOUT_S = 0.005
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
            yield await self._process_batch(requests)
            self.n_requests_in_flight -= len(requests)
            self.n_batches_in_flight -= 1

        async def _process_batch(self, requests):
            await asyncio.sleep(0.5)
            out = [(req_idx, self.n_batches_in_flight, self.n_requests_in_flight) for req_idx in requests]
            await asyncio.sleep(0.5)
            return out

        async def __call__(self, request):
            result = [item async for item in self.handle_batch(request)]
            assert len(result) == 1
            return result[0]

    handle = serve.run(BatchingExample.bind())

    idxs = set(range(20))
    result_futures = [handle.remote(i) for i in idxs]
    result_list = [future.result() for future in result_futures]

    for idx, batches_in_flight, requests_in_flight in result_list:
        idxs.remove(idx)
        assert batches_in_flight == BATCHES_IN_FLIGHT, (
            f"Should have been {BATCHES_IN_FLIGHT} batches in flight at all times, got {batches_in_flight}"
        )
        assert requests_in_flight == MAX_REQUESTS_IN_FLIGHT, (
            f"Should have been {MAX_REQUESTS_IN_FLIGHT} requests in flight at all times, got {requests_in_flight}"
        )

    assert len(idxs) == 0, "All requests should be processed"


if __name__ == "__main__":
    test_concurrent_batching()
    # asyncio.run(main2())
