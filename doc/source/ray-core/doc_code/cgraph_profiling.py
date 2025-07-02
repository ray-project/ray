import os
import subprocess
from pathlib import Path


NSIGHT_FAKE_DIR = str(
    Path(os.path.realpath(__file__)).parents[4]
    / "python"
    / "ray"
    / "tests"
    / "nsight_fake"
)


subprocess.check_call(
    ["pip", "install", NSIGHT_FAKE_DIR],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)


# __profiling_setup_start__
import ray
import torch
from ray.dag import InputNode


@ray.remote(num_gpus=1, runtime_env={"nsight": "default"})
class RayActor:
    def send(self, shape, dtype, value: int):
        return torch.ones(shape, dtype=dtype, device="cuda") * value

    def recv(self, tensor):
        return (tensor[0].item(), tensor.shape, tensor.dtype)


sender = RayActor.remote()
receiver = RayActor.remote()
# __profiling_setup_end__

# __profiling_execution_start__
shape = (10,)
dtype = torch.float16

# Test normal execution.
with InputNode() as inp:
    dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
    dag = dag.with_tensor_transport(transport="nccl")
    dag = receiver.recv.bind(dag)

compiled_dag = dag.experimental_compile()

for i in range(3):
    shape = (10 * (i + 1),)
    ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
    assert ray.get(ref) == (i, shape, dtype)
# __profiling_execution_end__

subprocess.check_call(
    ["pip", "uninstall", "nsys", "--y"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
