import ray
from ray import serve


@serve.deployment
class A:
    def __call__(self):
        signal = ray.get_actor("signal123")
        ray.get(signal.wait.remote())


app = A.bind()
