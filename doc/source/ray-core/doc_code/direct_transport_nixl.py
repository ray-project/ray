# flake8: noqa

# __nixl_full_example_start__
import torch
import ray

@ray.remote
class MyActor:
   @ray.method(tensor_transport="nixl")
   def random_tensor(self):
      return torch.randn(1000, 1000).cuda()

   def sum(self, tensor: torch.Tensor):
      return torch.sum(tensor)

sender, receiver = MyActor.remote(), MyActor.remote()

# The tensor will be stored by the `sender` actor instead of in Ray's object
# store.
tensor = sender.random_tensor.remote()
result = receiver.sum.remote(tensor)
# __nixl_full_example_end__

# __nixl_get_start__
# The :func:`ray.get <ray.get>` function will also use NIXL to retrieve the
# result.
print(ray.get(result))
# torch.Tensor(...)
# __nixl_get_end__