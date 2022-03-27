import ray
from ray import workflow

workflow.init()
@workflow.step
def identity(v):
    return v

@workflow.step
def w(i):
    s = 0
    for v in range(i):
        print("V=====", v)
        w = identity.options(num_retries=1).step(v)
        s += workflow.get(w)
        print(s)
    return s

print(w.options(num_retries=1).step(10).run())
