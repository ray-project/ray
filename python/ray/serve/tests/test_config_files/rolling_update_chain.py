"""Two deployments for testing rolling updates with a downstream failure."""

import os

from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class D1:
    def __init__(self, downstream: DeploymentHandle):
        self._downstream = downstream

    async def __call__(self, request):
        return await self._downstream.remote()


@serve.deployment
class D2:
    def __init__(self, version: str, fail: bool):
        if fail or os.environ.get("FAIL_ON_INIT") == "1":
            raise RuntimeError("downstream constructor failure requested by the test")
        self._version = os.environ.get("TEST_VERSION", version)

    def __call__(self):
        return self._version


def build(args):
    return D1.bind(D2.bind(args.get("version", "v1"), args.get("fail", False)))
