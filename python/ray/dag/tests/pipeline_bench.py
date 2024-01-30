# DO NOT MERGE

import ray
import time
from ray.dag import InputNode, MultiOutputNode


n_stages = 4
pipeline_concurrency = 6  # 1.5s/stage = 1s compute + 0.5s transfer
n = 100


@ray.remote
class Stage:
    def __init__(self, i):
        self.i = i

    def f(self, x):
        #        print("Process", x, self.i)
        time.sleep(1)
        if self.i == n_stages - 1:
            return b"last"
        else:
            return x + b"+"


# 4-stage pipeline
stages = [Stage.remote(i) for i in range(n_stages)]

with InputNode() as i:
    dag = i
    for stage in stages:
        dag = stage.f.bind(dag)

compiled_dag = dag.experimental_compile(buffer_size_bytes=100)
start = time.time()
active = []

for i in range(pipeline_concurrency):
    print("Executing", i)
    res = compiled_dag.execute(b"input")
    active.append(res)

# TODO need to submit / read from separate threads
for j in range(n - pipeline_concurrency):
    chn = active.pop(0)
    s = time.time()
    x = chn.begin_read()
    assert x == b"last", x
    chn.end_read()
    if time.time() - s > 0.2:
        print("*** Read delay ***", time.time() - s)
    print("Executing", j + pipeline_concurrency)
    s = time.time()
    res = compiled_dag.execute(b"input")
    if time.time() - s > 0.2:
        print("*** Execute delay ***", time.time() - s)
    active.append(res)
    print("iterations/s", (pipeline_concurrency + j) / (time.time() - start))

while active:
    print("Getting last result")
    chn = active.pop(0)
    chn.begin_read()
    chn.end_read()

print("iterations/s", n / (time.time() - start))
