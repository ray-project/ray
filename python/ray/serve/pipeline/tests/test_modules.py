"""
Ray decorated classes and functions defined at top of file, importable with
fully qualified name as import_path to test DAG building, artifact generation
and structured deployment.
"""
import ray

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
def fn_hello():
    return "hello"

@ray.remote
def combine(m1_output, m2_output, kwargs_output=0):
    return m1_output + m2_output + kwargs_output
