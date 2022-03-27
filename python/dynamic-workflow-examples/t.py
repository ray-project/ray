import ray
from ray import workflow

workflow.init()
from pathlib import Path
from filelock import FileLock
failure_flag = Path("/tmp/failure")

@workflow.step
def identity(v):
    if v == 5:
        if failure_flag.exists():
            print("Raise Excepton")
            raise RuntimeError()
    return v

@workflow.step
def w(i):
    s = 0
    return workflow.get(identity.step(5))

failure_flag.touch()

try:
    print(">>>>", w.step(10).run())
except RuntimeError:
    print("Failure")
