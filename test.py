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
        while True:
            buf = get_and_reset(self._input_ref)
            ray.worker.global_worker.put_object(buf * 2, object_ref=self._output_ref)
            # Delete ref because we cannot call ray.get on an unsealed ref.
            del buf


def get_and_reset(ref):
    # Wait for the DAG to finish.
    output_buf = ray.get(ref)
    # Reset the output ref.
    ray.worker.global_worker.core_worker.unseal_object(ref)
    return output_buf


a = Actor.remote()

input_ref = ray.put(np.zeros(100, dtype=np.uint8))
output_ref = ray.put(np.zeros(100, dtype=np.uint8))
print("Input:", input_ref)
print("Output:", output_ref)

ray.get([input_ref, output_ref])

# Initialization.
# - Give actor the references.
ray.get(a._init.remote([input_ref], [output_ref]))
# - Unseal all references.
get_and_reset(input_ref)
get_and_reset(output_ref)
# - All refs are unsealed. Start the actor loop.
a.execute.remote()

for i in range(1, 128):
    ray.worker.global_worker.put_object(np.ones(100, dtype=np.uint8) * i, object_ref=input_ref)

    output_buf = get_and_reset(output_ref)
    assert output_buf[0] == (i * 2), (i, output_buf)
    # Delete ref because we cannot call ray.get on an unsealed ref.
    del output_buf
