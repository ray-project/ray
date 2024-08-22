import ray
from ray.dag.input_node import InputNode
from ray.dag.output_node import MultiOutputNode
import time

@ray.remote
class Actor1:
    def fwd(self, x):
        time.sleep(1)

@ray.remote
class Actor2:
    def fwd(self, x):
        time.sleep(0.1)

    def bwd(self, x, y):
        time.sleep(1)

actor1 = Actor1.remote()
actor2 = Actor2.remote()

with InputNode() as input_node:
    ref1 = actor1.fwd.bind(input_node)
    ref2 = actor2.fwd.bind(input_node)
    dag = actor2.bwd.bind(ref1, ref2)
dag = dag.experimental_compile()

output_refs = []

s = time.time()
for i in range(10):
    output_refs.append(dag.execute(1))
ray.get(output_refs)
e = time.time()
print(f"Total run time: {e - s}")
