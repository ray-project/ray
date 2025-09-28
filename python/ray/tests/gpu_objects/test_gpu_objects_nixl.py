import sys

import pytest
import torch

import ray


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class GPUTestActor:
    @ray.method(tensor_transport="nixl")
    def echo(self, data, device):
        return data.to(device)

    def sum(self, data, device):
        assert data.device.type == device
        return data.sum().item()

    def produce(self, tensors):
        refs = []
        for t in tensors:
            refs.append(ray.put(t, _tensor_transport="nixl"))
        return refs

    def consume_with_nixl(self, refs):
        tensors = [ray.get(ref) for ref in refs]
        sum = 0
        for t in tensors:
            assert t.device.type == "cuda"
            sum += t.sum().item()
        return sum

    def consume_with_object_store(self, refs):
        tensors = [ray.get(ref, _tensor_transport="object_store") for ref in refs]
        sum = 0
        for t in tensors:
            assert t.device.type == "cuda"
            sum += t.sum().item()
        return sum

    def gc(self):
        tensor = torch.tensor([1, 2, 3]).to("cuda")
        ref = ray.put(tensor, _tensor_transport="nixl")
        gpu_manager = ray._private.worker.global_worker.gpu_object_manager
        assert gpu_manager.gpu_object_store.has_tensor(tensor)
        del ref
        gpu_manager.gpu_object_store.wait_tensor_freed(tensor, timeout=10)
        assert not gpu_manager.gpu_object_store.has_tensor(tensor)
        return "Success"


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_ray_get_gpu_ref_created_by_actor_task(ray_start_regular):
    actor = GPUTestActor.remote()
    tensor = torch.tensor([1, 2, 3]).to("cuda")
    ref1 = actor.echo.remote(tensor, "cuda")
    ref2 = actor.echo.remote(tensor, "cuda")
    ref3 = actor.echo.remote(tensor, "cuda")

    # Test ray.get with default tensor transport, should use nixl here.
    # TODO: Verify it's using the correct tensor transport.
    assert torch.equal(ray.get(ref1), tensor)

    # # Test ray.get with nixl tensor transport
    assert torch.equal(ray.get(ref2, _tensor_transport="nixl"), tensor)

    # # Test ray.get with object store tensor transport
    assert torch.equal(ray.get(ref3, _tensor_transport="object_store"), tensor)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_p2p(ray_start_regular):
    num_actors = 2
    actors = [GPUTestActor.remote() for _ in range(num_actors)]

    src_actor, dst_actor = actors[0], actors[1]

    # Create test tensor
    tensor = torch.tensor([1, 2, 3])

    tensor1 = torch.tensor([4, 5, 6])

    # Test GPU to GPU transfer
    ref = src_actor.echo.remote(tensor, "cuda")

    # Trigger tensor transfer from src to dst actor
    result = dst_actor.sum.remote(ref, "cuda")
    assert tensor.sum().item() == ray.get(result)

    # Test CPU to CPU transfer
    ref1 = src_actor.echo.remote(tensor1, "cpu")
    result1 = dst_actor.sum.remote(ref1, "cpu")
    assert tensor1.sum().item() == ray.get(result1)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_intra_gpu_tensor_transfer(ray_start_regular):
    actor = GPUTestActor.remote()

    tensor = torch.tensor([1, 2, 3])

    # Intra-actor communication for pure GPU tensors
    ref = actor.echo.remote(tensor, "cuda")
    result = actor.sum.remote(ref, "cuda")
    assert tensor.sum().item() == ray.get(result)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_put_and_get_object_with_nixl(ray_start_regular):
    actors = [GPUTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]
    tensor1 = torch.tensor([1, 2, 3]).to("cuda")
    tensor2 = torch.tensor([4, 5, 6, 0]).to("cuda")
    tensor3 = torch.tensor([7, 8, 9, 0, 0]).to("cuda")
    tensors = [tensor1, tensor2, tensor3]
    ref = src_actor.produce.remote(tensors)
    ref1 = dst_actor.consume_with_nixl.remote(ref)
    result1 = ray.get(ref1)
    assert result1 == 45


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_put_and_get_object_with_object_store(ray_start_regular):
    actors = [GPUTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]
    tensor1 = torch.tensor([1, 2, 3]).to("cuda")
    tensor2 = torch.tensor([4, 5, 6, 0]).to("cuda")
    tensor3 = torch.tensor([7, 8, 9, 0, 0]).to("cuda")
    tensors = [tensor1, tensor2, tensor3]
    ref = src_actor.produce.remote(tensors)
    ref1 = dst_actor.consume_with_object_store.remote(ref)
    result1 = ray.get(ref1)
    assert result1 == 45


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_put_gc(ray_start_regular):
    actor = GPUTestActor.remote()
    ref = actor.gc.remote()
    assert ray.get(ref) == "Success"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
