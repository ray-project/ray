# flake8: noqa
# __streaming_example_start__
# File name: stream.py
from typing import AsyncGenerator, Generator

from ray import serve
from ray.serve.handle import DeploymentHandle, DeploymentResponseGenerator


@serve.deployment
class Streamer:
    def __call__(self, limit: int) -> Generator[int, None, None]:
        for i in range(limit):
            yield i


@serve.deployment
class Caller:
    def __init__(self, streamer: DeploymentHandle):
        self._streamer = streamer.options(
            # Must set `stream=True` on the handle, then the output will be a
            # response generator.
            stream=True,
        )

    async def __call__(self, limit: int) -> AsyncGenerator[int, None]:
        # Response generator can be used in an `async for` block.
        r: DeploymentResponseGenerator = self._streamer.remote(limit)
        async for i in r:
            yield i


app = Caller.bind(Streamer.bind())

handle: DeploymentHandle = serve.run(app).options(
    stream=True,
)

# Response generator can also be used as a regular generator in a sync context.
r: DeploymentResponseGenerator = handle.remote(10)
assert list(r) == list(range(10))
# __streaming_example_end__
