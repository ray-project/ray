# DO NOT MERGE

import ray
import time
from ray.dag import InputNode


n_stages = 4
pipeline_concurrency = 7  # 1.5s/stage = 1s compute + 0.5s transfer
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

pipeline = dag.experimental_compile(
    buffer_size_bytes=100, max_concurrency=pipeline_concurrency
)
start = time.time()

for i in range(pipeline_concurrency):
    print("Executing", i)
    pipeline.execute(b"input")
print("Warmup done")

# TODO need to submit / read from separate threads
for j in range(n - pipeline_concurrency):
    s = time.time()
    with pipeline.get_next() as x:
        assert x == b"last", x
    if time.time() - s > 0.2:
        print("*** Read delay ***", time.time() - s)
    print("Executing", j + pipeline_concurrency)
    s = time.time()
    pipeline.execute(b"input")
    if time.time() - s > 0.2:
        print("*** Execute delay ***", time.time() - s)
    print("iterations/s", (pipeline_concurrency + j) / (time.time() - start))

while pipeline.has_next():
    print("Getting last result")
    with pipeline.next_result() as result:
        pass

print("iterations/s", n / (time.time() - start))
