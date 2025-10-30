import sys

import pytest
import torch

import ray


@ray.remote(num_cpus=0.5, enable_tensor_transport=True)
class DSTestActor:
    def __init__(self):
        from ray.util.collective.collective import _group_mgr
        from ray.util.collective.collective_group.ds_backend import DSBackend
        from ray.util.collective.types import DS_GROUP_NAME

        mock_backend = DSBackend()
        _group_mgr._name_group_map[DS_GROUP_NAME] = mock_backend
        _group_mgr._group_name_map[mock_backend] = DS_GROUP_NAME

    @ray.method(tensor_transport="ds")
    def echo(self, data):
        return data

    def sum(self, data):
        return data.sum().item()

    def produce(self, tensors):
        refs = []
        for t in tensors:
            refs.append(ray.put(t, _tensor_transport="ds"))
        return refs

    def consume_with_ds(self, refs):
        tensors = [ray.get(ref) for ref in refs]
        sum_val = 0
        for t in tensors:
            assert t.device.type == "cpu"
            sum_val += t.sum().item()
        return sum_val

    def consume_with_object_store(self, refs):
        tensors = [ray.get(ref, _tensor_transport="object_store") for ref in refs]
        sum_val = 0
        for t in tensors:
            assert t.device.type == "cpu"
            sum_val += t.sum().item()
        return sum_val

    def gc(self):
        tensor = torch.tensor([1, 2, 3])
        ref = ray.put(tensor, _tensor_transport="ds")
        gpu_manager = ray._private.worker.global_worker.gpu_object_manager
        assert gpu_manager.gpu_object_store.has_tensor(tensor)
        del ref
        gpu_manager.gpu_object_store.wait_tensor_freed(tensor, timeout=10)
        assert not gpu_manager.gpu_object_store.has_tensor(tensor)
        return "Success"


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_ray_get_ds_ref_created_by_actor_task(ray_start_regular):
    actor = DSTestActor.remote()
    tensor = torch.tensor([1, 2, 3])
    ref1 = actor.echo.remote(tensor)
    ref2 = actor.echo.remote(tensor)
    ref3 = actor.echo.remote(tensor)

    result1 = ray.get(ref1)
    assert torch.equal(result1, torch.tensor([1, 1, 1]))

    result2 = ray.get(ref2, _tensor_transport="ds")
    assert torch.equal(result2, torch.tensor([1, 1, 1]))

    result3 = ray.get(ref3, _tensor_transport="object_store")
    assert torch.equal(result3, tensor)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_p2p(ray_start_regular):
    num_actors = 2
    actors = [DSTestActor.remote() for _ in range(num_actors)]

    src_actor, dst_actor = actors[0], actors[1]
    tensor = torch.tensor([1, 2, 3])
    ref = src_actor.echo.remote(tensor)

    result = dst_actor.sum.remote(ref)
    assert ray.get(result) == 3


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 1}], indirect=True)
def test_intra_tensor_transfer(ray_start_regular):
    actor = DSTestActor.remote()
    tensor = torch.tensor([1, 2, 3])
    ref = actor.echo.remote(tensor)
    result = actor.sum.remote(ref)
    assert ray.get(result) == 6


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_put_and_get_object_with_ds(ray_start_regular):
    actors = [DSTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]

    tensor1 = torch.tensor([1, 2, 3])
    tensor2 = torch.tensor([4, 5, 6, 0])
    tensor3 = torch.tensor([7, 8, 9, 0, 0])
    tensors = [tensor1, tensor2, tensor3]

    ref = src_actor.produce.remote(tensors)
    ref1 = dst_actor.consume_with_ds.remote(ref)
    result1 = ray.get(ref1)
    assert result1 == 12


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_put_and_get_object_with_object_store(ray_start_regular):
    actors = [DSTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]

    tensor1 = torch.tensor([1, 2, 3])
    tensor2 = torch.tensor([4, 5, 6, 0])
    tensor3 = torch.tensor([7, 8, 9, 0, 0])
    tensors = [tensor1, tensor2, tensor3]

    ref = src_actor.produce.remote(tensors)
    ref1 = dst_actor.consume_with_object_store.remote(ref)
    result1 = ray.get(ref1)
    assert result1 == 45


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 1}], indirect=True)
def test_put_gc(ray_start_regular):
    actor = DSTestActor.remote()
    ref = actor.gc.remote()
    assert ray.get(ref) == "Success"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
