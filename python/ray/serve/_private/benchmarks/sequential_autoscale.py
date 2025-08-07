# File name: hello.py
import ray
from ray import serve

ray.init()
serve.start(http_options={"host": "0.0.0.0", "port": 8081})


@serve.deployment(
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "target_num_ongoing_requests_per_replica": 2,
        "upscale_delay_s": 2,
        "downscale_delay_s": 10,
    }
)
class A:
    def __init__(self, b):
        self.b = b

    async def __call__(self):
        print("Foo")
        response = self.b.remote()
        return await response


@serve.deployment(
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "target_num_ongoing_requests_per_replica": 2,
        "upscale_delay_s": 2,
        "downscale_delay_s": 10,
    }
)
class B:
    def __init__(self, c):
        self.c = c

    async def __call__(self):
        print("Bar")
        response = self.c.remote()
        return await response


@serve.deployment(
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "target_num_ongoing_requests_per_replica": 2,
        "upscale_delay_s": 2,
        "downscale_delay_s": 10,
    }
)
class C:
    def __init__(self):
        self.c = c

    def __call__(self):
        print("Baz")
        return "Baz"


c = C.bind()
b = B.bind(c)
a = A.bind(b)

serve.run(a, route_prefix="/")

while True:
    pass
