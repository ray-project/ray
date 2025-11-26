import sys

import pytest
import torch

import ray


@ray.remote(enable_tensor_transport=True)
class GPUTestActor:
    def __init__(self):
        self.tensor = None

    @ray.method(tensor_transport="cuda_ipc")
    def echo(self, data):
        self.tensor = data.to("cuda")
        return self.tensor

    def double(self, data):
        data.mul_(2)
        torch.cuda.synchronize()
        return data

    def wait_tensor_freed(self):
        gpu_manager = ray.worker.global_worker.gpu_object_manager
        ray.experimental.wait_tensor_freed(self.tensor, timeout=10)
        assert not gpu_manager.gpu_object_store.has_tensor(self.tensor)
        return "freed"


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_colocated_actors(ray_start_regular):
    world_size = 2
    actors = [
        GPUTestActor.options(num_gpus=0.5, num_cpus=0).remote()
        for _ in range(world_size)
    ]

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)

    # Trigger tensor transfer from src to dst actor
    ray.get(dst_actor.double.remote(gpu_ref))
    # Check that the tensor is modified in place, and is reflected on the source actor
    assert torch.equal(
        ray.get(gpu_ref, _tensor_transport="object_store"),
        torch.tensor([2, 4, 6], device="cuda"),
    )


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_ipc_fail(ray_start_regular):
    world_size = 2
    actors = [
        GPUTestActor.options(num_gpus=1, num_cpus=0).remote() for _ in range(world_size)
    ]

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)

    # Trigger tensor transfer from src to dst actor. Since CUDA IPC transport does not
    # support cross-device tensor transfers, this should raise a ValueError.
    with pytest.raises(ValueError):
        ray.get(dst_actor.double.remote(gpu_ref))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))


def test_ipc_with_original_ref_freed(ray_start_regular):
    world_size = 2
    actors = [
        GPUTestActor.options(num_gpus=0.5, num_cpus=0).remote()
        for _ in range(world_size)
    ]

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)

    # Trigger tensor transfer from src to dst actor
    res_ref = dst_actor.double.remote(gpu_ref)

    del gpu_ref

    free_res = ray.get(src_actor.wait_tensor_freed.remote())
    assert free_res == "freed"

    assert torch.equal(
        ray.get(res_ref),
        torch.tensor([2, 4, 6], device="cuda"),
    )
