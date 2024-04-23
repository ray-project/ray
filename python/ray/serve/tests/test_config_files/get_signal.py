import ray
from ray import serve


@serve.deployment
class A:
    async def __call__(self):
        signal = ray.get_actor("signal123")
        await signal.wait.remote()


app = A.bind()
