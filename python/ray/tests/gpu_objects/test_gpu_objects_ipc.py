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
        ray.get(gpu_ref, _use_object_store=True),
        torch.tensor([2, 4, 6], device="cuda"),
    )


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_different_devices(ray_start_regular):
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
    with pytest.raises(
        ValueError, match="CUDA IPC transport only supports tensors on the same GPU*"
    ):
        ray.get(dst_actor.double.remote(gpu_ref))


def test_different_nodes(ray_start_cluster):
    # Test that inter-node CUDA IPC transfers throw an error.
    cluster = ray_start_cluster
    num_nodes = 2
    num_cpus = 1
    num_gpus = 1
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpus, num_gpus=num_gpus)
    ray.init(address=cluster.address)

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
    with pytest.raises(
        ValueError, match="CUDA IPC transport only supports tensors on the same node.*"
    ):
        ray.get(dst_actor.double.remote(gpu_ref))


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_ref_freed(ray_start_regular):
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
        ray.get(res_ref, _use_object_store=True),
        torch.tensor([2, 4, 6], device="cuda"),
    )


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_source_actor_fails_after_transfer(ray_start_regular):
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
    assert torch.equal(
        ray.get(res_ref, _use_object_store=True),
        torch.tensor([2, 4, 6], device="cuda"),
    )

    # Kill the source actor.
    ray.kill(src_actor)
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(src_actor.wait_tensor_freed.remote())

    # Check that the tensor is still available on the destination actor.
    assert torch.equal(
        ray.get(res_ref, _use_object_store=True),
        torch.tensor([2, 4, 6], device="cuda"),
    )


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_source_actor_fails_before_transfer(ray_start_regular):
    world_size = 2
    actors = [
        GPUTestActor.options(num_gpus=0.5, num_cpus=0).remote()
        for _ in range(world_size)
    ]

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])
    gpu_ref = src_actor.echo.remote(tensor)

    # Wait for object to be created.
    assert torch.equal(
        ray.get(gpu_ref, _use_object_store=True),
        torch.tensor([1, 2, 3], device="cuda"),
    )

    # Kill the source actor.
    ray.kill(src_actor)
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(src_actor.wait_tensor_freed.remote())

    # Check that the tensor is still available on the destination actor.
    with pytest.raises(ray.exceptions.RayTaskError):
        res_ref = dst_actor.double.remote(gpu_ref)
        ray.get(res_ref, _use_object_store=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
