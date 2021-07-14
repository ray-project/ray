import sys
import time
import ray
from ray.experimental import workflow


@workflow.step
def foo(x):
    print("Executing", x)
    time.sleep(1)
    if x < 20:
        return foo.step(x + 1)
    else:
        return 20


if __name__ == "__main__":
    sleep_duration = float(sys.argv[1])
    ray.init(address="auto", namespace="workflow")
    wf = workflow.run(foo.step(0), workflow_id="driver_terminated")
    time.sleep(sleep_duration)
