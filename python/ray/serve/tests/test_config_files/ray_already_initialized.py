import ray
from ray import serve

ray.init(address="auto")


@serve.deployment
def f():
    return "foobar"


app = f.bind()
