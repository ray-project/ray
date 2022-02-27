"""
Ray decorated classes and functions defined at top of file, importable with
fully qualified name as import_path to test DAG building, artifact generation
and structured deployment.
"""
import ray
from typing import TypeVar

RayHandleLike = TypeVar("RayHandleLike")
NESTED_HANDLE_KEY = "nested_handle"


@ray.remote
class ClassHello:
    def __init__(self):
        pass

    def hello(self):
        return "hello"


@ray.remote
class Model:
    def __init__(self, weight: int, ratio: float = None):
        self.weight = weight
        self.ratio = ratio or 1

    def forward(self, input: int):
        print(f"forward() recevied input: {input}")
        return self.ratio * self.weight * input

    def __call__(self, request):
        print(f"__call__() recevied request: {request}")
        input_data = request
        return self.ratio * self.weight * input_data


@ray.remote
class Combine:
    def __init__(
        self,
        m1: "RayHandleLike",
        m2: "RayHandleLike" = None,
        m2_nested: bool = False,
    ):
        self.m1 = m1
        self.m2 = m2.get(NESTED_HANDLE_KEY) if m2_nested else m2

    def __call__(self, req):
        r1_ref = self.m1.forward.remote(req)
        r2_ref = self.m2.forward.remote(req)
        return sum(ray.get([r1_ref, r2_ref]))


@ray.remote
def fn_hello():
    return "hello"


@ray.remote
def combine(m1_output, m2_output):
    return m1_output + m2_output
