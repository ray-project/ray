import ray
import cupy as cp

import ray.util.collective as collective


@ray.remote(num_gpus=1)
class Worker:
    def __init__(self):
        self.recv = cp.zeros((4 ), dtype=cp.float32)
        self.send = [cp.ones((4 ), dtype=cp.float32), cp.ones((4 ), dtype=cp.float32)]

    def setup(self, world_size, rank):
        self.send = [a * (rank + 1) for a in self.send]
        collective.init_collective_group(world_size, rank, "nccl", "default")
        return True

    def compute(self):
        collective.reducescatter(self.recv, self.send, group_name = "default")
        return self.recv

    def destroy(self):
        collective.destroy_group()


if __name__ == "__main__":

    send = cp.ones((4, ), dtype=cp.float32)

    ray.init(num_gpus=2)

    num_workers = 2
    workers = []
    init_rets = []
    for i in range(num_workers):
        w = Worker.remote()
        workers.append(w)
        init_rets.append(w.setup.remote(num_workers, i))
    _ = ray.get(init_rets)
    results = ray.get([w.compute.remote() for w in workers])
    print(results)
    ray.shutdown()
