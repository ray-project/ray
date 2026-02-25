# flake8: noqa

# __begin_per_handle__
from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class Downstream:
    def __call__(self, request):
        return "Hello from downstream"


@serve.deployment
class Upstream:
    def __init__(self, downstream: DeploymentHandle):
        # Enable gRPC transport for this handle
        self._downstream = downstream.options(_by_reference=False)

    async def __call__(self, request):
        return await self._downstream.remote()


app = Upstream.bind(Downstream.bind())
# __end_per_handle__


# __begin_serialization_options__
# Configure serialization format per-handle.
# Use msgpack or orjson for better performance when data is simple
# (dicts, lists, strings, numbers).
handle = downstream.options(  # noqa: F821
    _by_reference=False,
    request_serialization="msgpack",
    response_serialization="msgpack",
)
# __end_serialization_options__
