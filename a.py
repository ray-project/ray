import ray
from ray.dag.vis_utils import plot
ray.init()

from ray.dag.input_node import InputNode

@ray.remote
def a(user_input):
    return user_input * 2

@ray.remote
def b(user_input):
    return user_input + 1

@ray.remote
def c(x, y):
    return x + y

with InputNode() as dag_input:
    a_ref = a.bind(dag_input)
    b_ref = b.bind(dag_input)
    dag = c.bind(a_ref, b_ref)

#   a(2)  +   b(2)  = c
# (2 * 2) + (2 * 1)
assert ray.get(dag.execute(2)) == 7

#   a(3)  +   b(3)  = c
# (3 * 2) + (3 * 1)
assert ray.get(dag.execute(3)) == 10
