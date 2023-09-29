import ray
import numpy as np


@ray.remote
class Actor:
    def __init__(self):
        pass

    def _init(self, input_refs, output_refs):
        self._input_ref = input_refs[0]
        self._output_ref = output_refs[0]

    def execute(self):
        buf = ray.get(self._input_ref)
        last = 0
        while True:
            # Hack to find out when the object has been unsealed and recreated.
            cur = buf[0]
            if cur != last:
                ray.worker.global_worker.put_object(buf * 2, object_ref=self._output_ref)
            last = cur


def wait_for_completion_and_reset(input_ref, output_ref):
    # Wait for the DAG to finish.
    output_buf = ray.get(output_ref)

    # Reset the input and output refs.
    # NOTE(swang): Have the driver unseal because we don't have a way to signal
    # the actor when the object has been unsealed. Alternatively, the actor
    # should unseal its input ref BEFORE sealing the output ref.
    ray.worker.global_worker.core_worker.unseal_object(input_ref)
    ray.worker.global_worker.core_worker.unseal_object(output_ref)
    return output_buf


a = Actor.remote()

input_ref = ray.put(np.zeros(100, dtype=np.uint8))
output_ref = ray.put(np.zeros(100, dtype=np.uint8))

# Initialization.
# - Give actor the references.
# - Unseal all references.
# - All refs are unsealed. Start the actor loop.
ray.get(a._init.remote([input_ref], [output_ref]))
# Get the input buf because we need to be using the object in order to unseal
# it.
input_buf = ray.get(input_ref)
wait_for_completion_and_reset(input_ref, output_ref)
a.execute.remote()

for i in range(1, 10):
    ray.worker.global_worker.put_object(np.ones(100, dtype=np.uint8) * i, object_ref=input_ref)

    output_buf = wait_for_completion_and_reset(input_ref, output_ref)
    assert output_buf[0] == i * 2
    # Delete ref because we cannot call ray.get on an unsealed ref.
    del output_buf
