import numpy as np
import ray
from ray._private.ray_microbenchmark_helpers import timeit
from ray.dag import InputNode
from ray.experimental.channel.shared_memory_channel import SharedMemoryType

SHAPE = 100

@ray.remote
class Worker:
    def __init__(self):
        pass

    def send(self, value):
        return value

    def recv(self, value):
        return value

if __name__ == "__main__":
    sender = Worker.remote()
    receiver = Worker.remote()

    # Test torch.Tensor sent between actors.
    with InputNode() as inp:
        dag = sender.send.bind(inp)
        dag.with_type_hint(SharedMemoryType(transport=SharedMemoryType.GLOO))
        dag = receiver.recv.bind(dag)

    dag = dag.experimental_compile()
    data = np.ones(SHAPE, dtype=np.float32)
    ref = dag.execute(data)
    print(ray.get(ref))
    print("done")
