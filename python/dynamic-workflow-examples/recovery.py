import ray
from ray import workflow
ray.init(namespace="x")
workflow.init()
from pathlib import Path
from filelock import FileLock
failure_flag = Path("/tmp/failure")
import time

@workflow.step
def identity(v):
    time.sleep(1)
    if v % 2 == 0:
        raise RuntimeError()
    return v

@workflow.step
def double(v):
    time.sleep(1)
    return v * 2


@workflow.step
def s(v):
    s = 0
    ss = 0
    for i in range(v):
        print(f"--- in iteration {i} ---")
        try:
            # this also work
            # s += workflow.get(identity.step(i))
            # ss += workflow.get(double.step(i))

            # wait in parallel
            x = workflow.get([identity.step(i), double.step(i)])
            print(x)
            s1, ss1 = x
            s += s1
            ss += ss1
        except RuntimeError:
            pass

        if i > 5 and failure_flag.exists():
            raise RuntimeError()
    return (s, ss)

import uuid
workflow_id = str(uuid.uuid1())
failure_flag.touch()

print("JOB", workflow_id)
n = time.time()
try:
    print(">>>>>>>>>> Finished", s.step(10).run(workflow_id=workflow_id))
except Exception:
    print("====== Finished with error")
print("cost", time.time() -  n)

failure_flag.unlink()
from time import sleep
sleep(3)
print("Start to resume")
n = time.time()
try:
    print(">>>>>>>>>>", ray.get(workflow.resume(workflow_id)))
except Exception:
    print("====== Finished with error")
print("cost", time.time() - n)
