# flake8: noqa

# fmt: off
# __dag_tasks_begin__
import ray

ray.init()

@ray.remote
def func(src, inc=1):
    return src + inc

a_ref = func.bind(1, inc=2)
assert ray.get(a_ref.execute()) == 3 # 1 + 2 = 3
b_ref = func.bind(a_ref, inc=3)
assert ray.get(b_ref.execute()) == 6 # (1 + 2) + 3 = 6
c_ref = func.bind(b_ref, inc=a_ref)
assert ray.get(c_ref.execute()) == 9 # ((1 + 2) + 3) + (1 + 2) = 9
# __dag_tasks_end__
# fmt: on

ray.shutdown()

# fmt: off
# __dag_actors_begin__
import ray

ray.init()

@ray.remote
class Actor:
    def __init__(self, init_value):
        self.i = init_value

    def inc(self, x):
        self.i += x

    def get(self):
        return self.i

a1 = Actor.bind(10)  # Instantiate Actor with init_value 10.
val = a1.get.bind()  # ClassMethod that returns value from get() from
                     # the actor created.
assert ray.get(val.execute()) == 10

@ray.remote
def combine(x, y):
    return x + y

a2 = Actor.bind(10) # Instantiate another Actor with init_value 10.
a1.inc.bind(2)  # Call inc() on the actor created with increment of 2.
a1.inc.bind(4)  # Call inc() on the actor created with increment of 4.
a2.inc.bind(6)  # Call inc() on the actor created with increment of 6.

# Combine outputs from a1.get() and a2.get()
dag = combine.bind(a1.get.bind(), a2.get.bind())

# a1 +  a2 + inc(2) + inc(4) + inc(6)
# 10 + (10 + ( 2   +    4    +   6)) = 32
assert ray.get(dag.execute()) == 32
# __dag_actors_end__
# fmt: on

ray.shutdown()

# fmt: off
# __dag_input_node_begin__
import ray

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
# __dag_input_node_end__
# fmt: on
