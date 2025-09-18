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
group = create_collective_group([sender, receiver], backend="torch_gloo")
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
group = create_collective_group([sender, receiver], backend="torch_gloo")

# The tensor will be stored by the `sender` actor instead of in Ray's object
# store.
tensor = sender.random_tensor.remote()
result = receiver.sum.remote(tensor)
print(ray.get(result))
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
group = create_collective_group([sender, receiver], backend="torch_gloo")

# Both tensor values in the dictionary will be stored by the `sender` actor
# instead of in Ray's object store.
tensor_dict = sender.random_tensor_dict.remote()
result = receiver.sum.remote(tensor_dict)
print(ray.get(result))
# __gloo_multiple_tensors_example_end__

# __gloo_intra_actor_start__
import torch
import ray
import pytest
from ray.experimental.collective import create_collective_group


@ray.remote
class MyActor:
    @ray.method(tensor_transport="gloo")
    def random_tensor(self):
        return torch.randn(1000, 1000)

    def sum(self, tensor: torch.Tensor):
        return torch.sum(tensor)


sender, receiver = MyActor.remote(), MyActor.remote()
group = create_collective_group([sender, receiver], backend="torch_gloo")

tensor = sender.random_tensor.remote()
# Pass the ObjectRef back to the actor that produced it. The tensor will be
# passed back to the same actor without copying.
sum1 = sender.sum.remote(tensor)
sum2 = receiver.sum.remote(tensor)
assert torch.allclose(*ray.get([sum1, sum2]))
# __gloo_intra_actor_end__

# __gloo_get_start__

# Wrong example of ray.get(). Since the tensor transport in the @ray.method decorator is Gloo,
# ray.get() will try to use Gloo to fetch the tensor, which is not supported
# because the caller is not part of the collective group.
with pytest.raises(ValueError) as e:
    ray.get(tensor)

assert (
    "Currently ray.get() only supports OBJECT_STORE and NIXL tensor transport, got TensorTransportEnum.GLOO, please specify the correct tensor transport in ray.get()"
    in str(e.value)
)

# Correct example of ray.get(), explicitly setting the tensor transport to use the Ray object store.
print(ray.get(tensor, _tensor_transport="object_store"))
# torch.Tensor(...)
# __gloo_get_end__
