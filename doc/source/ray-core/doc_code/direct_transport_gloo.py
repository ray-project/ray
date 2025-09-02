# flake8: noqa

# __normal_example_start__
import torch
import ray

@ray.remote
class MyActor:
   def random_tensor(self):
      return torch.randn(1000, 1000)
# __normal_example_end__

# __gloo_example_start__
@ray.remote
class MyActor:
   @ray.method(tensor_transport="gloo")
   def random_tensor(self):
      return torch.randn(1000, 1000)
# __gloo_example_end__

# __gloo_group_start__
import torch
import ray
from ray.experimental.collective import create_collective_group

@ray.remote
class MyActor:
   @ray.method(tensor_transport="gloo")
   def random_tensor(self):
      return torch.randn(1000, 1000)
    
   def sum(self, tensor: torch.Tensor):
      return torch.sum(tensor)

sender, receiver = MyActor.remote(), MyActor.remote()
# The tensor_transport specified here must match the one used in the @ray.method
# decorator.
group = create_collective_group([sender, receiver], tensor_transport="gloo")
# __gloo_group_end__

# __gloo_group_destroy_start__
from ray.experimental.collective import destroy_collective_group

destroy_collective_group(group)
# __gloo_group_destroy_end__

# __gloo_full_example_start__
import torch
import ray
from ray.experimental.collective import create_collective_group

@ray.remote
class MyActor:
   @ray.method(tensor_transport="gloo")
   def random_tensor(self):
      return torch.randn(1000, 1000)

   def sum(self, tensor: torch.Tensor):
      return torch.sum(tensor)

sender, receiver = MyActor.remote(), MyActor.remote()
group = create_collective_group([sender, receiver], tensor_transport="gloo")

# The tensor will be stored by the `sender` actor instead of in Ray's object
# store.
tensor = sender.random_tensor.remote()
result = receiver.sum.remote(tensor)
print(result)
# __gloo_full_example_end__

# __gloo_multiple_tensors_example_start__
import torch
import ray
from ray.experimental.collective import create_collective_group

@ray.remote
class MyActor:
   @ray.method(tensor_transport="gloo")
   def random_tensor_dict(self):
      return {"tensor1": torch.randn(1000, 1000), "tensor2": torch.randn(1000, 1000)}

   def sum(self, tensor_dict: dict):
      return torch.sum(tensor_dict["tensor1"]) + torch.sum(tensor_dict["tensor2"])

sender, receiver = MyActor.remote(), MyActor.remote()
group = create_collective_group([sender, receiver], tensor_transport="gloo")

# Both tensor values in the dictionary will be stored by the `sender` actor
# instead of in Ray's object store.
tensor_dict = sender.random_tensor_dict.remote()
result = receiver.sum.remote(tensor_dict)
print(result)
# __gloo_multiple_tensors_example_end__

# __gloo_intra_actor_start__
tensor = sender.random_tensor.remote()
sum1 = sender.sum.remote(tensor)
sum2 = receiver.sum.remote(tensor)
assert torch.allclose(ray.get([sum1, sum2]))
# __gloo_intra_actor_end__

# __gloo_get_start__
print(ray.get(tensor))
# torch.Tensor(...)
# __gloo_get_end__