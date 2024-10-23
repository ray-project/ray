# coding: utf-8
import os
import sys

import pytest

import ray
from ray.dag import InputNode, MultiOutputNode
import ray.cluster_utils
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.tests.conftest import *  # noqa
import torch

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_multi_args_simulate_pp(ray_start_regular):
    if not USE_GPU:
        pytest.skip("NCCL tests require GPUs")

    @ray.remote(num_cpus=0, num_gpus=1)
    class Worker:
        def __init__(self):
            pass

        def forward(self, data):
            return data

        def backward(self, data):
            return data

    NUM_MICROBATCHES = 2
    w0 = Worker.remote()
    w1 = Worker.remote()
    with InputNode() as dag_input:
        dag_outs = []
        for microbatch_idx in range(NUM_MICROBATCHES):
            microbatch = dag_input[microbatch_idx]
            stage_fwd_out = w0.forward.bind(microbatch)
            stage_fwd_out.with_type_hint(TorchTensorType(transport="nccl"))
            stage_fwd_out = w1.forward.bind(stage_fwd_out)
            dag_outs.append(stage_fwd_out)

        grad_out = dag_input[NUM_MICROBATCHES]
        for _ in range(NUM_MICROBATCHES):
            stage_bwd_out = w1.backward.bind(grad_out)
            stage_bwd_out.with_type_hint(TorchTensorType(transport="nccl"))
            stage_bwd_out = w0.backward.bind(stage_bwd_out)
            dag_outs.append(stage_bwd_out)

        dag = MultiOutputNode(dag_outs)
    compiled_dag = dag.experimental_compile()

    tensor_cpu_list = [torch.zeros(1, i + 1) for i in range(3)]
    tensor_cuda_list = [t.to("cuda:0") for t in tensor_cpu_list]
    ref = compiled_dag.execute(
        tensor_cuda_list[0], tensor_cuda_list[1], tensor_cuda_list[2]
    )
    tensors = ray.get(ref)

    assert len(tensors) == 4
    assert torch.equal(tensors[0], tensor_cpu_list[0])
    assert torch.equal(tensors[1], tensor_cpu_list[1])
    assert torch.equal(tensors[2], tensor_cpu_list[2])
    assert torch.equal(tensors[3], tensor_cpu_list[2])

    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
