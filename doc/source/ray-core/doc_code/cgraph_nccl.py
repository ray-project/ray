# __cgraph_cpu_to_gpu_actor_start__
import torch
import ray
import ray.dag


@ray.remote(num_gpus=1)
class GPUActor:
    def process(self, tensor: torch.Tensor):
        assert tensor.device.type == "cuda"
        return tensor.shape


actor = GPUActor.remote()
# __cgraph_cpu_to_gpu_actor_end__

# __cgraph_cpu_to_gpu_start__
with ray.dag.InputNode() as inp:
    inp = inp.with_tensor_transport(device="cuda")
    dag = actor.process.bind(inp)

cdag = dag.experimental_compile()
print(ray.get(cdag.execute(torch.zeros(10))))
# __cgraph_cpu_to_gpu_end__

# __cgraph_cpu_to_gpu_override_start__
from ray.experimental.channel import ChannelContext


@ray.remote(num_gpus=1)
class GPUActor:
    def __init__(self):
        # Set the default device to CPU.
        ctx = ChannelContext.get_current()
        ctx.set_torch_device(torch.device("cpu"))

    def process(self, tensor: torch.Tensor):
        assert tensor.device.type == "cpu"
        return tensor.shape


# __cgraph_cpu_to_gpu_override_end__

# __cgraph_nccl_setup_start__
import torch
import ray
import ray.dag
from ray.experimental.channel.torch_tensor_type import TorchTensorType


# Note that the following example requires at least 2 GPUs.
assert (
    ray.available_resources().get("GPU") >= 2
), "At least 2 GPUs are required to run this example."


@ray.remote(num_gpus=1)
class GPUSender:
    def send(self, shape):
        return torch.zeros(shape, device="cuda")


@ray.remote(num_gpus=1)
class GPUReceiver:
    def recv(self, tensor: torch.Tensor):
        assert tensor.device.type == "cuda"
        return tensor.shape


sender = GPUSender.remote()
receiver = GPUReceiver.remote()
# __cgraph_nccl_setup_end__

# __cgraph_nccl_exec_start__
with ray.dag.InputNode() as inp:
    dag = sender.send.bind(inp)
    # Add a type hint that the return value of `send` should use NCCL.
    dag = dag.with_tensor_transport("nccl")
    # NOTE: With ray<2.42, use `with_type_hint()` instead.
    # dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
    dag = receiver.recv.bind(dag)

# Compile API prepares the NCCL communicator across all workers and schedule operations
# accordingly.
dag = dag.experimental_compile()
assert ray.get(dag.execute((10,))) == (10,)
# __cgraph_nccl_exec_end__
