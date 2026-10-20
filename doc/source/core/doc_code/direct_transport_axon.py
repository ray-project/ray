# __axon_example_start__
import numpy as np

import ray

ray.init(num_cpus=2, include_dashboard=False)


@ray.remote(enable_tensor_transport=True)
class ArrayActor:
    @ray.method(tensor_transport="axon")
    def produce(self):
        return np.arange(1024, dtype=np.float32)

    def consume(self, array):
        return float(array.sum())


sender, receiver = ArrayActor.remote(), ArrayActor.remote()
ref = sender.produce.remote()
assert ray.get(receiver.consume.remote(ref)) == 523776.0
np.testing.assert_array_equal(ray.get(ref), np.arange(1024, dtype=np.float32))
ray.shutdown()
# __axon_example_end__
