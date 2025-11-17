# flake8: noqa

# __ds_full_example_start__
import torch
import torch_npu
import ray


@ray.remote(resources={"NPU": 1})
class MyActor:
    @ray.method(tensor_transport="ds")
    def random_tensor(self):
        return torch.randn(1000, 1000).npu()

    def sum(self, tensor: torch.Tensor):
        return torch.sum(tensor)

    def produce(self, tensors):
        refs = []
        for t in tensors:
            refs.append(ray.put(t, _tensor_transport="ds"))
        return refs

    def consume_with_ds(self, refs):
        # ray.get will also use ds to retrieve the
        # result.
        tensors = [ray.get(ref) for ref in refs]
        sum = 0
        for t in tensors:
            assert t.device.type == "npu"
            sum += t.sum().item()
        return sum


sender, receiver = MyActor.remote(), MyActor.remote()

# The tensor will be stored by the `sender` actor instead of in Ray's object
# store.
tensor = sender.random_tensor.remote()
result = receiver.sum.remote(tensor)
print(ray.get(result))
# __ds_full_example_end__

# __ds_get_start__
# ray.get will also use ds to retrieve the
# result.
print(ray.get(tensor))
torch.Tensor(...)
# __ds_get_end__

# __ds_put__and_get_start__
tensor1 = torch.randn(1000, 1000).npu()
tensor2 = torch.randn(1000, 1000).npu()
refs = sender.produce.remote([tensor1, tensor2])
ref1 = receiver.consume_with_ds.remote(refs)
print(ray.get(ref1))
# __ds_put__and_get_end__
