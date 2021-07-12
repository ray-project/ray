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
    ray.init(address="auto")
    wf = workflow.run(foo.step(0), workflow_id="cluster_failure")
    assert ray.get(wf) == 20
