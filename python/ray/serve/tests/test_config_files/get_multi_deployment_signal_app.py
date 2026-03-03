import os

import ray
from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class A:
    def __init__(self, b: DeploymentHandle):
        self.b = b
        self.signal = ray.get_actor("signal_A", namespace="default_test_namespace")

    async def __call__(self):
        await self.signal.wait.remote()
        return os.getpid()


@serve.deployment
class B:
    def __init__(self):
        self.signal = ray.get_actor("signal_B", namespace="default_test_namespace")

    async def __call__(self):
        await self.signal.wait.remote()
        return os.getpid()


app = A.bind(B.bind())
