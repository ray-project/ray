# flake8: noqa

# __nixl_full_example_start__
import torch
import ray


@ray.remote(num_gpus=1)
class MyActor:
    @ray.method(tensor_transport="nixl")
    def random_tensor(self):
        return torch.randn(1000, 1000).cuda()

    def sum(self, tensor: torch.Tensor):
        return torch.sum(tensor)

    def produce(self, tensors):
        refs = []
        for t in tensors:
            refs.append(ray.put(t, _tensor_transport="nixl"))
        return refs

    def consume_with_nixl(self, refs):
        # ray.get will also use NIXL to retrieve the
        # result.
        tensors = [ray.get(ref) for ref in refs]
        sum = 0
        for t in tensors:
            assert t.device.type == "cuda"
            sum += t.sum().item()
        return sum


# No collective group is needed. The two actors just need to have NIXL
# installed.
sender, receiver = MyActor.remote(), MyActor.remote()

# The tensor will be stored by the `sender` actor instead of in Ray's object
# store.
tensor = sender.random_tensor.remote()
result = receiver.sum.remote(tensor)
ray.get(result)
# __nixl_full_example_end__

# __nixl_get_start__
# ray.get will also use NIXL to retrieve the
# result.
print(ray.get(tensor))
# torch.Tensor(...)
# __nixl_get_end__

# __nixl_put__and_get_start__
tensor1 = torch.randn(1000, 1000).cuda()
tensor2 = torch.randn(1000, 1000).cuda()
refs = sender.produce.remote([tensor1, tensor2])
ref1 = receiver.consume_with_nixl.remote(refs)
print(ray.get(ref1))
# __nixl_put__and_get_end__
