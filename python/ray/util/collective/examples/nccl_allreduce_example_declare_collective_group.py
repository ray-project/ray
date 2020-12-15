import cupy as cp
import ray

import ray.util.collective as collective

@ray.remote(num_gpus=1)
class Worker:
    def __init__(self):
        self.send = cp.ones((4,), dtype=cp.float32)

    def compute(self):
        collective.allreduce(self.send, '177')
        return self.send

    def destroy(self):
        collective.destroy_group('')

if __name__ == "__main__":
    ray.init(num_gpus=2)

    num_workers = 2
    workers = []
    for i in range(num_workers):
        w = Worker.remote()
        workers.append(w)
    options = {'group_name' : '177',
               'world_size' : 2,
               'rank' : [0, 1],
               'backend' : 'nccl'}
    collective.declare_collective_group(workers, options)
    results = ray.get([w.compute.remote() for w in workers])
    print(results)
    ray.shutdown()
