import ray
import cupy as cp

import ray.util.collective as collective
from cupy.cuda import Device


@ray.remote(num_gpus=2)
class Worker:
    def __init__(self):
        with Device(0):
            self.send1 = cp.ones((4,), dtype=cp.float32)
        with Device(1):
            self.send2 = cp.ones((4,), dtype=cp.float32) * 2

        with Device(0):
            self.recv1 = cp.zeros((4,), dtype=cp.float32)
        with Device(1):
            self.recv2 = cp.zeros((4,), dtype=cp.float32)
        self.rank = -1

    def setup(self, world_size, rank):
        self.rank = rank
        collective.init_collective_group(world_size, rank, "nccl", "8")
        return True

    def compute(self):
        if self.rank == 0:
            with Device(0):
                collective.send_multigpu(self.send1 * 2, 1, 1, "8")
        else:
            # with Device(1):
            collective.recv_multigpu(self.recv2, 0, 0, "8")
        return self.recv2

    def destroy(self):
        collective.destroy_collective_group("8")


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
