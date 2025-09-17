import ray
import torch
from ray.experimental.collective import create_collective_group

ray.init()

@ray.remote(num_gpus=1, enable_tensor_transport=True)
class Worker:
    @ray.method(tensor_transport="nccl")
    def process(self):
        return {"tensor": [torch.tensor([1, 2, 3], device="cuda"), torch.tensor([4, 5, 6], device="cuda")], "name": ["string1", "string2"]}

    def finish(self, dict):
        return dict["tensor"]

@ray.remote(num_gpus=1)
class Worker2:
    def process(self):
        return {"tensor": [torch.tensor([1, 2, 3], device="cuda"), torch.tensor([4, 5, 6], device="cuda")], "name": ["string1", "string2"]}

    def finish(self, dict):
        return dict["tensor"]


class Manager:
    def __init__(self):
        self.worker2 = Worker2.remote()

    def invoke(self, dict):
        ray.get(self.worker2.finish.remote(dict))

        

if __name__ == "__main__":
    manager = Manager()
    worker1 = Worker.remote()
    extra_worker = Worker.remote()
    # worker2 = Worker2.remote()
    create_collective_group([worker1, manager.worker2, extra_worker], backend="nccl")
    # tensor = torch.tensor([1, 2, 3], device="cuda")
    ref = worker1.process.remote()
    manager.invoke(ref)
    # ray.get(manager.invoke.remote(worker1, worker2))