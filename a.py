import ray
from ray.dag import InputNode, MultiOutputNode

ray.init()

@ray.remote
class A:
    def f(self, inp):
        raise RuntimeError

a = A.remote()
b = A.remote()
with InputNode() as inp:
    x = b.f.bind(inp)
    y = a.f.bind(inp)
    dag = MultiOutputNode([x, y])
adag = dag.experimental_compile()
x, y = adag.execute(1)

# Should print unhandled error.
del x
del y