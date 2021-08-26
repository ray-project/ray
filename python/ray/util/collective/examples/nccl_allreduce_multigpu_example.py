import ray
import cupy as cp

import ray.util.collective as collective
from cupy.cuda import Device


@ray.remote(num_gpus=2)
class Worker:
    def __init__(self):
        with Device(0):
            self.send1 = cp.ones((4, ), dtype=cp.float32)
        with Device(1):
            self.send2 = cp.ones((4, ), dtype=cp.float32) * 2

        self.recv = cp.zeros((4, ), dtype=cp.float32)

    def setup(self, world_size, rank):
        collective.init_collective_group(world_size, rank, "nccl", "177")
        return True

    def compute(self):
        collective.allreduce_multigpu([self.send1, self.send2], "177")
        return [self.send1, self.send2], self.send1.device, self.send2.device

    def destroy(self):
        collective.destroy_collective_group("177")


if __name__ == "__main__":
    ray.init(address="auto")
    num_workers = 2
    workers = []
    init_rets = []
    for i in range(num_workers):
        w = Worker.remote()
        workers.append(w)
        init_rets.append(w.setup.remote(num_workers, i))
    a = ray.get(init_rets)
    results = ray.get([w.compute.remote() for w in workers])
    print(results)
    ray.get([w.destroy.remote() for w in workers])
    ray.shutdown()
