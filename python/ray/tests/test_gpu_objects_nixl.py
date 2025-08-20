import sys
import torch
import pytest
import ray


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class GPUTestActor:
    @ray.method(tensor_transport="nixl")
    def echo_gpu(self, data):
        return data.to("cuda")

    def sum_gpu(self, data):
        assert data.device.type == "cuda"
        return data.sum().item()

    @ray.method(tensor_transport="nixl")
    def echo_cpu(self, data):
        return data.to("cpu")

    def sum_cpu(self, data):
        assert data.device.type == "cpu"
        return data.sum().item()


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_p2p(ray_start_regular):
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])

    tensor1 = torch.tensor([4, 5, 6])

    # Test GPU to GPU transfer
    ref = src_actor.echo_gpu.remote(tensor)

    # Trigger tensor transfer from src to dst actor
    result = dst_actor.sum_gpu.remote(ref)
    assert tensor.sum().item() == ray.get(result)

    # Test CPU to CPU transfer
    ref1 = src_actor.echo_cpu.remote(tensor1)
    result1 = dst_actor.sum_cpu.remote(ref1)
    assert tensor1.sum().item() == ray.get(result1)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_intra_gpu_tensor_transfer(ray_start_regular):
    actor = GPUTestActor.remote()

    tensor = torch.tensor([1, 2, 3])

    # Intra-actor communication for pure GPU tensors
    ref = actor.echo_gpu.remote(tensor)
    result = actor.sum_gpu.remote(ref)
    assert tensor.sum().item() == ray.get(result)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
