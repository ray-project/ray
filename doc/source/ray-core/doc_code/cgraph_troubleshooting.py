# __numpy_troubleshooting_start__
import ray
import numpy as np
from ray.dag import InputNode


@ray.remote
class NumPyActor:
    def get_arr(self, _):
        numpy_arr = np.ones((5, 1024))
        return numpy_arr


actor = NumPyActor.remote()
with InputNode() as inp:
    dag = actor.get_arr.bind(inp)
cgraph = dag.experimental_compile()
for _ in range(5):
    ref = cgraph.execute(0)
    result = ray.get(ref)
    # Adding this explicit del would fix any issues
    # del result
# __numpy_troubleshooting_end__

# __teardown_troubleshooting_start__
@ray.remote
class SendActor:
    def send(self, x):
        return x


actor = SendActor.remote()
with InputNode() as inp:
    dag = actor.send.bind(inp)
cgraph = dag.experimental_compile()

# Not adding this explicit teardown before reusing `actor` could cause problems
# cgraph.teardown()

with InputNode() as inp:
    dag = actor.send.bind(inp)
cgraph = dag.experimental_compile()
# __teardown_troubleshooting_end__
