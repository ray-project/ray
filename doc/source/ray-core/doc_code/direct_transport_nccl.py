# flake8: noqa

# __nccl_full_example_start__
import torch
import ray
from ray.experimental.collective import create_collective_group

@ray.remote
class MyActor:
   @ray.method(tensor_transport="nccl")
   def random_tensor(self):
      return torch.randn(1000, 1000).cuda()

   def sum(self, tensor: torch.Tensor):
      return torch.sum(tensor)

sender, receiver = MyActor.remote(), MyActor.remote()
group = create_collective_group([sender, receiver], tensor_transport="nccl")

# The tensor will be stored by the `sender` actor instead of in Ray's object
# store.
tensor = sender.random_tensor.remote()
result = receiver.sum.remote(tensor)
# __nccl_full_example_end__