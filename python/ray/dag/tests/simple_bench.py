# DO NOT MERGE

import ray
import time
from ray.dag import InputNode, MultiOutputNode


@ray.remote
class A:
    def f(self, x):
        return b"result"


workers = [A.remote() for _ in range(4)]

with InputNode() as i:
    dag = MultiOutputNode([a.f.bind(i) for a in workers])

compiled_dag = dag.experimental_compile(buffer_size_bytes=100)
start = time.time()
n = 1000
for _ in range(n):
    channels = compiled_dag.execute(b"input")
    for chn in channels:
        chn.begin_read()
        chn.end_read()

# iterations/s 8263.400948823428
print("iterations/s", n / (time.time() - start))
