# flake8: noqa

from ray import serve
from ray.serve.handle import DeploymentHandle

# __start_grpc_override__
@serve.deployment
class Caller:
    def __init__(self, target: DeploymentHandle):
        # Override this specific handle to use actor RPC instead of gRPC.
        # This is useful for large payloads (over ~1 MB) where passing
        # objects by reference through Ray's object store is more efficient.
        self._target = target.options(_by_reference=True)

    async def __call__(self, data: bytes) -> str:
        return await self._target.remote(data)


@serve.deployment
class LargePayloadProcessor:
    def __call__(self, data: bytes) -> str:
        return f"processed {len(data)} bytes"


processor = LargePayloadProcessor.bind()
app = Caller.bind(processor)

handle: DeploymentHandle = serve.run(app)
assert handle.remote(b"x" * 1024).result() == "processed 1024 bytes"
# __end_grpc_override__

serve.shutdown()
