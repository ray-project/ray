"""
Ray decorated classes and functions defined at top of file, importable with
fully qualified name as import_path to test DAG building, artifact generation
and structured deployment.
"""
import asyncio
from typing import TypeVar

from ray import serve
from ray.actor import ActorHandle

RayHandleLike = TypeVar("RayHandleLike")
NESTED_HANDLE_KEY = "nested_handle"


@serve.deployment
class ClassHello:
    def __init__(self):
        pass

    def hello(self):
        return "hello"


@serve.deployment
class Model:
    def __init__(self, weight: int, ratio: float = None):
        self.weight = weight
        self.ratio = ratio or 1

    def forward(self, input: int):
        return self.ratio * self.weight * input

    def __call__(self, request):
        input_data = request
        return self.ratio * self.weight * input_data


@serve.deployment
class Combine:
    def __init__(
        self,
        m1: "RayHandleLike",
        m2: "RayHandleLike" = None,
        m2_nested: bool = False,
    ):
        self.m1 = m1
        self.m2 = m2.get(NESTED_HANDLE_KEY) if m2_nested else m2

    async def __call__(self, req):
        if isinstance(self.m1, ActorHandle) and isinstance(self.m2, ActorHandle):
            r1_ref = self.m1.forward.remote(req)
            r2_ref = self.m2.forward.remote(req)
        else:
            r1_ref = await self.m1.forward.remote(req)
            r2_ref = await self.m2.forward.remote(req)

        return sum(await asyncio.gather(r1_ref, r2_ref))


@serve.deployment
class Counter:
    def __init__(self, val):
        self.val = val

    def get(self):
        return self.val

    def inc(self, inc):
        self.val += inc


@serve.deployment
def fn_hello():
    return "hello"


@serve.deployment
def fn(val, incr=0):
    return val + incr


@serve.deployment
def combine(m1_output, m2_output, kwargs_output=0):
    return m1_output + m2_output + kwargs_output


def class_factory():
    class MyInlineClass:
        def __init__(self, val):
            self.val = val

        def get(self):
            return self.val

    return MyInlineClass
