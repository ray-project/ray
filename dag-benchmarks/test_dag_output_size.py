import os
import time

import ray

from ray.dag import InputNode, OutputNode
from ray.dag.compiled_dag_node import RayCompiledExecutor


@ray.remote
class Actor(RayCompiledExecutor):
    def __init__(self, init_value):
        print("__init__ PID", os.getpid())
        self.i = init_value

    def get(self, x):
        return x

    def inc(self, x):
        return x + b"1"


init_val = 10
actors = [Actor.remote(init_val) for _ in range(1)]

with InputNode() as i:
    out = [a.inc.bind(i) for a in actors]
    dag = OutputNode(out)

for _ in range(10):
    input_val = b"hello"
    refs = dag.execute(input_val, compiled=True)
    val = ray.get(refs)[0]
    assert input_val + b"1" == val
    for ref in refs:
        ray.release(ref)

# TODO: NOT working.
# Test different outputs chained.
# with InputNode() as i:
#     out = [a.inc.bind(i) for a in actors]
#     for _ in range(3):
#         out = [a.inc.bind(o) for o, a in zip(out, actors)]
#     dag = OutputNode(out)

# for _ in range(3):
#     input_val = b"hello"
#     expected = b"hello111"
#     refs = dag.execute(input_val, compiled=True)
#     val = ray.get(refs)[0]
#     assert expected == val
#     for ref in refs:
#         ray.release(ref)
