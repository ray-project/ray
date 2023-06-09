# flake8: noqa
# __single_sample_begin__
import ray
from ray import serve


@serve.deployment
class Model:
    def __call__(self, single_sample: int) -> int:
        return single_sample * 2


handle = serve.run(Model.bind())
assert ray.get(handle.remote(1)) == 2
# __single_sample_end__


# __batch_begin__
import numpy as np
from typing import List

import ray
from ray import serve


@serve.deployment
class Model:
    @serve.batch(max_batch_size=8, batch_wait_timeout_s=0.1)
    async def __call__(self, multiple_samples: List[int]) -> List[int]:
        # Use numpy's vectorized computation to efficiently process a batch.
        return np.array(multiple_samples) * 2


handle = serve.run(Model.bind())
assert ray.get([handle.remote(i) for i in range(8)]) == [i * 2 for i in range(8)]
# __batch_end__


# __single_stream_begin__
import asyncio
from typing import AsyncGenerator
from starlette.requests import Request
from starlette.responses import StreamingResponse

from ray import serve


@serve.deployment
class StreamingResponder:
    async def generate_numbers(self, max: int) -> AsyncGenerator[int, None]:
        for i in range(max):
            yield i
            asyncio.sleep(0.1)

    def __call__(self, request: Request) -> StreamingResponse:
        max = int(request.query_params.get("max", "25"))
        gen = self.generate_numbers(max)
        return StreamingResponse(gen, status_code=200, media_type="text/plain")


# __single_stream_end__

# TODO (shrekris-anyscale): add unit tests

# __batch_stream_begin__
import asyncio
from typing import List, AsyncGenerator
from starlette.requests import Request
from starlette.responses import StreamingResponse

from ray import serve


@serve.deployment
class StreamingResponder:
    @serve.batch(max_batch_size=5, batch_wait_timeout_s=15)
    async def generate_numbers(
        self, max_list: List[int]
    ) -> AsyncGenerator[List[int], None]:
        for i in range(max(max_list)):
            next_numbers = []
            for requested_max in max_list:
                if requested_max > i:
                    next_numbers.append(i)
                else:
                    next_numbers.append(StopIteration)
            yield next_numbers
            asyncio.sleep(0.1)

    def __call__(self, request: Request) -> StreamingResponse:
        max = int(request.query_params.get("max", "25"))
        gen = self.generate_numbers(max)
        return StreamingResponse(gen, status_code=200, media_type="text/plain")


# __batch_stream_end__

# TODO (shrekris-anyscale): add unit tests
