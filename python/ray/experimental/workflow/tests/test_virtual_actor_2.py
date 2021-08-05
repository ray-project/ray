import time
import ray
import pytest

from ray.tests.conftest import *  # noqa
from ray.experimental import workflow


@workflow.virtual_actor
class Counter:
    def __init__(self, x: int):
        self.x = x

    @workflow.virtual_actor.readonly
    def readonly_get(self):
        return self.x

    @workflow.virtual_actor.readonly
    def readonly_incr(self):
        self.x += 1
        return self.x

    def add(self, y):
        self.x += y
        return self.x

    def __getstate__(self):
        return self.x

    def __setstate__(self, state):
        self.x = state


@workflow.virtual_actor
class IndirectCounter:
    def __init__(self, x):
        actor = Counter.get_or_create("counter", x)
        ray.get(actor.ready())

    @workflow.virtual_actor.readonly
    def readonly_get(self):
        actor = workflow.get_actor("counter")
        return actor.readonly_get.run()

    @workflow.virtual_actor.readonly
    def readonly_incr(self):
        actor = workflow.get_actor("counter")
        return actor.readonly_incr.run()

    def add(self, y):
        actor = workflow.get_actor("counter")
        return actor.add.run(y)

    @workflow.virtual_actor.readonly
    def readonly_workload(self):
        # simulate a workload
        time.sleep(1)

    def __getstate__(self):
        return

    def __setstate__(self, state):
        pass


@pytest.mark.parametrize(
    "workflow_start_regular",
    [{
        "num_cpus": 4
        # We need more CPUs, otherwise 'create()' blocks 'get()'
    }],
    indirect=True)
def test_indirect_actor_writer(workflow_start_regular):
    actor = IndirectCounter.get_or_create("indirect_counter", 0)
    ray.get(actor.ready())
    assert actor.readonly_get.run() == 0
    array = []
    s = 0
    for i in range(1, 10):
        s += i
        array.append(s)
    assert [actor.add.run(i) for i in range(1, 10)] == array
    assert actor.readonly_get.run() == 45

    array = []
    for i in range(10, 20):
        s += i
        array.append(s)
    assert ray.get([actor.add.run_async(i) for i in range(10, 20)]) == array


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
