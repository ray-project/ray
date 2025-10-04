import os

import ray
from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class A:
    def __init__(self, b: DeploymentHandle):
        self.b = b

    async def __call__(self):
        signal = ray.get_actor("signal_A")
        await signal.wait.remote()
        return os.getpid()


@serve.deployment
class B:
    async def __call__(self):
        signal = ray.get_actor("signal_B")
        await signal.wait.remote()
        return os.getpid()


app = A.bind(B.bind())
