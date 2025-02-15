import ray
import torch
import logging
import numpy as np
from ray.dag import InputNode


ray.init(logging_level=logging.DEBUG, log_to_driver=True)


@ray.remote(num_gpus=1)
class Actor:
    def send(self, value: int, device: str):
        return torch.ones(10, device=device) * value

    def recv(self, tensor):
        return (tensor[0].item(), str(tensor.device))


# sender = Actor.remote()


# # Test torch.Tensor sent between actors.
# with InputNode() as inp:
#     dag = sender.send.bind(inp[0], inp[1])

# compiled_dag = dag.experimental_compile()
# ref = compiled_dag.execute(1, "cuda")
# print(ray.get(ref))

# ref = compiled_dag.execute(2, "cpu")
# print(ray.get(ref))


@ray.remote(num_gpus=1)
class A:
    def f(self, x: int):
        return torch.tensor([x], device="cuda")

    def to_cpu(self, x: torch.Tensor):
        return x.cpu()


a = A.remote()

# with InputNode() as inp:
#     out = a.f.bind(inp)
#     dag = a.to_cpu.bind(out)

# compiled_dag = dag.experimental_compile()
# for i in range(3):
#     ref = compiled_dag.execute(i)
#     result = ray.get(ref)
#     assert result == i

# compiled_dag.teardown()

# If we don't move to cpu on worker, it will raise an error.
with InputNode() as inp:
    dag = a.f.bind(inp)

compiled_dag = dag.experimental_compile()
ref = compiled_dag.execute(1)
print(ray.get(ref))

# with pytest.raises(ray.exceptions.RayAdagDeviceMismatchError):
#     ray.get(ref)
# breakpoint()
