from ray import serve
from ray.serve.drivers import DAGDriver
from ray.dag.input_node import InputNode

"""
We are building a DAG like this:
A ->  B ----> C
 \->  D --/
  \-> E -/
"""


@serve.deployment
def a(val: int):
    return val


@serve.deployment
def b(val: int):
    return val + 1


@serve.deployment
def c(v1: int, v2: int, v3: int):
    return sum([v1, v2, v3])


@serve.deployment
def d(val):
    return val + 2


@serve.deployment
def e(val):
    return val + 3


with InputNode() as user_input:
    oa = a.bind(user_input)
    ob = b.bind(oa)
    od = d.bind(oa)
    oe = e.bind(oa)
    oc = c.bind(ob, od, oe)


def input_adapter(val: int):
    return val


serve_entrypoint = DAGDriver.bind(oc, http_adapter=input_adapter)
