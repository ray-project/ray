import sys

import pytest
import torch

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.experimental.rdt.util import get_tensor_transport_manager


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class GPUTestActor:
    def __init__(self):
        self.reserved_tensor1 = torch.tensor([1, 2, 3]).to("cuda")
        self.reserved_tensor2 = torch.tensor([4, 5, 6]).to("cuda")
        self.reserved_tensor3 = torch.tensor([7, 8, 9]).to("cuda")

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
        tensors = [ray.get(ref, _use_object_store=True) for ref in refs]
        sum = 0
        for t in tensors:
            assert t.device.type == "cuda"
            sum += t.sum().item()
        return sum

    def gc(self):

        tensor = torch.tensor([1, 2, 3]).to("cuda")
        ref = ray.put(tensor, _tensor_transport="nixl")
        obj_id = ref.hex()
        gpu_manager = ray._private.worker.global_worker.gpu_object_manager
        nixl_transport = get_tensor_transport_manager("NIXL")

        assert gpu_manager.gpu_object_store.has_tensor(tensor)
        assert gpu_manager.is_managed_object(obj_id)
        assert obj_id in nixl_transport._managed_meta_nixl
        # Tensor-level metadata counting: the tensor should have metadata_count=1
        key = tensor.untyped_storage().data_ptr()
        assert key in nixl_transport._tensor_desc_cache
        assert nixl_transport._tensor_desc_cache[key].metadata_count == 1

        del ref

        gpu_manager.gpu_object_store.wait_tensor_freed(tensor, timeout=10)
        assert not gpu_manager.gpu_object_store.has_tensor(tensor)
        assert not gpu_manager.is_managed_object(obj_id)
        assert obj_id not in nixl_transport._managed_meta_nixl
        assert key not in nixl_transport._tensor_desc_cache
        return "Success"

    @ray.method(tensor_transport="nixl")
    def send_dict1(self):
        return {"round1-1": self.reserved_tensor1, "round1-2": self.reserved_tensor2}

    @ray.method(tensor_transport="nixl")
    def send_dict2(self):
        return {"round2-1": self.reserved_tensor1, "round2-3": self.reserved_tensor3}

    def sum_dict(self, dict):
        return sum(v.sum().item() for v in dict.values())

    def get_num_gpu_objects(self):
        gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
        return gpu_object_manager.gpu_object_store.get_num_objects()

    def get_num_managed_meta_nixl(self):

        return get_tensor_transport_manager("NIXL")._get_num_managed_meta_nixl()

    def put_shared_tensor_lists(self):
        """Create two tensor lists that share a common tensor and put them with NIXL transport."""
        t1 = torch.tensor([1, 2, 3]).to("cuda")
        t2 = torch.tensor([4, 5, 6]).to("cuda")
        t3 = torch.tensor([7, 8, 9]).to("cuda")

        list1 = [t1, t2]
        list2 = [t2, t3]

        ref1 = ray.put(list1, _tensor_transport="nixl")
        # Nixl itself doesn't handle duplicate memory registrations,
        # hence this call would fail without proper deduplication.
        ref2 = ray.put(list2, _tensor_transport="nixl")

        return ref1, ref2

    @ray.method(concurrency_group="_ray_system")
    def block_background_thread(self, signal_actor):
        ray.get(signal_actor.wait.remote())

    def borrow_and_sum(self, ref_list):
        return ray.get(ref_list[0]).sum().item()

    def block_main_thread(self, signal_actor):
        ray.get(signal_actor.wait.remote())


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
    assert torch.equal(ray.get(ref2), tensor)

    # # Test ray.get with object store tensor transport
    assert torch.equal(ray.get(ref3, _use_object_store=True), tensor)


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


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_send_duplicate_tensor(ray_start_regular):
    actors = [GPUTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]
    ref1 = src_actor.send_dict1.remote()
    result1 = dst_actor.sum_dict.remote(ref1)
    assert ray.get(result1) == 21
    ref2 = src_actor.send_dict1.remote()
    result2 = dst_actor.sum_dict.remote(ref2)
    assert ray.get(result2) == 21

    del ref1
    del ref2
    wait_for_condition(
        lambda: ray.get(src_actor.get_num_gpu_objects.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )
    wait_for_condition(
        lambda: ray.get(src_actor.get_num_managed_meta_nixl.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_abort_sender_dies_before_sending(ray_start_regular):
    actors = [GPUTestActor.remote() for _ in range(2)]

    """
    1. Block background thread on receiver so receive doesn't start
    2. Wait until the object is created so the transfer gets triggered
    3. Kill the sender
    4. Unblock the receiver
    """
    signal_actor = SignalActor.remote()
    actors[1].block_background_thread.remote(signal_actor)
    ref = actors[0].echo.remote(torch.randn((100, 100)), "cuda")
    result = actors[1].sum.remote(ref, "cuda")
    ray.wait([ref])
    ray.kill(actors[0])
    signal_actor.send.remote()

    with pytest.raises(ray.exceptions.RayTaskError) as excinfo:
        ray.get(result)

    exc_str = str(excinfo.value)
    assert "nixlBackendError" in exc_str and "The source actor may have died" in exc_str

    # Try a transfer with actor[1] receiving again
    new_actor = GPUTestActor.remote()
    ref = new_actor.echo.remote(torch.tensor([4, 5, 6]), "cuda")
    result = actors[1].sum.remote(ref, "cuda")
    assert ray.get(result) == 15


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_del_before_creating(ray_start_regular):
    """
    Blocking the main thread until we free the object from the reference counter.
    Then unblocking the actor's main thread so the object can be created and then
    asserting that the object was actually freed.
    """
    signal_actor = SignalActor.remote()
    actor = GPUTestActor.remote()
    actor.block_main_thread.remote(signal_actor)
    ref = actor.echo.remote(torch.tensor([4, 5, 6]), "cuda")
    obj_id = ref.hex()
    del ref
    ray.get(signal_actor.send.remote())

    wait_for_condition(
        lambda: ray._private.worker.global_worker.gpu_object_manager.get_gpu_object_metadata(
            obj_id
        )
        is None,
    )
    wait_for_condition(
        lambda: ray.get(actor.get_num_gpu_objects.remote()) == 0,
    )


@pytest.mark.skip(
    "If the tensor metadata doesn't exist at the time of borrowing, this will fail."
)
@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_borrow_after_abort(ray_start_regular):
    actors = [GPUTestActor.remote() for _ in range(2)]
    nixl_ref = actors[0].echo.remote(torch.tensor([4, 5, 6]), "cuda")
    assert ray.get(actors[1].borrow_and_sum.remote([nixl_ref])) == 15


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_shared_tensor_deduplication(ray_start_regular):
    """
    Test that tensors shared across multiple lists are properly deduplicated.

    Creates list1 = [T1, T2] and list2 = [T2, T3] where T2 is shared.
    """
    actor = GPUTestActor.remote()
    ray.get(actor.put_shared_tensor_lists.remote())


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_agent_reuse(ray_start_regular):
    """
    We reuse nixl remote agent by default. The receiver should successfully receive
    all tensors while the sender may trigger GC in between.
    """
    actors = [GPUTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]

    ref1 = src_actor.echo.remote(torch.tensor([1, 2, 3]).to("cuda"), "cuda")
    assert ray.get(dst_actor.sum.remote(ref1, "cuda")) == 6

    # Trigger another transfer. The receiver successfully gets
    # the latest tensor (nixl agent is reused internally).
    ref2 = src_actor.echo.remote(torch.tensor([4, 5, 6]).to("cuda"), "cuda")
    assert ray.get(dst_actor.sum.remote(ref2, "cuda")) == 15

    del ref1, ref2

    # Wait for GC to free the tensors on the sender.
    wait_for_condition(
        lambda: ray.get(src_actor.get_num_managed_meta_nixl.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )

    # Transfer after GC. The receiver successfully gets
    # the latest tensor (nixl agent is reset internally).
    ref3 = src_actor.echo.remote(torch.tensor([7, 8, 9]).to("cuda"), "cuda")
    assert ray.get(dst_actor.sum.remote(ref3, "cuda")) == 24


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_agent_reuse_with_partial_tensors(ray_start_regular):
    """
    We reuse nixl remote agent by default. The receiver should successfully choose
    and receive part of the tensors.
    """
    actors = [GPUTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]

    ref1 = src_actor.echo.remote(torch.tensor([1, 2, 3, 4, 5, 6]).to("cuda"), "cuda")
    assert ray.get(dst_actor.sum.remote(ref1, "cuda")) == 21

    del ref1

    # Wait for GC to free the tensors on the sender.
    wait_for_condition(
        lambda: ray.get(src_actor.get_num_managed_meta_nixl.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )

    # Create the second tensor at the sender. The memory address of
    # this tensor may overlap with the first tensor (de-registered).
    ref2 = src_actor.echo.remote(torch.tensor([1, 2, 3]).to("cuda"), "cuda")

    # Create the third tensor at the sender. The memory address of
    # this tensor may overlap with the first tensor (de-registered).
    ref3 = src_actor.echo.remote(torch.tensor([4, 5, 6]).to("cuda"), "cuda")
    # Trigger the transfer. The receiver successfully gets
    # the third tensor (nixl agent is reset internally).
    assert ray.get(dst_actor.sum.remote(ref3, "cuda")) == 15

    del ref2, ref3


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_storage_level_overlapping_views_reference_count(ray_start_regular):
    """Test that two overlapping tensors sharing the same underlying storage produce a
    single NIXL registration. When each tensor's ref goes out of scope via
    garbage_collect, the metadata_count decrements. After both are freed,
    the registration is removed."""
    from ray.experimental.rdt.nixl_tensor_transport import (
        NixlTensorTransport,
    )

    transport = NixlTensorTransport()

    tensor = torch.tensor([[1, 1], [2, 2], [3, 3]], dtype=torch.float32).to("cuda")
    view0 = tensor[0:2]
    view1 = tensor[1:3]
    storage_key = tensor.untyped_storage().data_ptr()

    assert view0.untyped_storage().data_ptr() == storage_key
    assert view1.untyped_storage().data_ptr() == storage_key
    assert view0.data_ptr() != view1.data_ptr()

    # Simulate ray.put(view0)
    obj_id1 = "test_obj_id_1"
    meta1 = transport.extract_tensor_transport_metadata(obj_id1, [view0])
    assert len(transport._tensor_desc_cache) == 1
    assert transport._tensor_desc_cache[storage_key].metadata_count == 1

    # Simulate ray.put(view1) and check that the a new entry is not created in the tensor desc cache
    # since they share the same storage key and the metadata_count is incremented by 1
    obj_id2 = "test_obj_id_2"
    meta2 = transport.extract_tensor_transport_metadata(obj_id2, [view1])
    assert len(transport._tensor_desc_cache) == 1
    assert transport._tensor_desc_cache[storage_key].metadata_count == 2

    # Simulate the obj ref for view0 going out of scope and check that the nixl memory registration is
    # not cleared since the object ref for view1 is still in scope
    transport.garbage_collect(obj_id1, meta1, [view0])
    assert storage_key in transport._tensor_desc_cache
    assert transport._tensor_desc_cache[storage_key].metadata_count == 1

    # Simulate the obj ref for view1 going out of scope and check that the nixl memory registration is cleared
    transport.garbage_collect(obj_id2, meta2, [view1])
    assert storage_key not in transport._tensor_desc_cache


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class OverlappingViewProducer:
    def produce_overlapping_views(self):
        tensor = torch.tensor([1, 2, 3, 4, 5], dtype=torch.float32).to("cuda")
        slices = [tensor[0:2], tensor[1:3], tensor[2:4]]
        refs = []
        for s in slices:
            refs.append(ray.put(s, _tensor_transport="nixl"))
        return refs


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_storage_level_overlapping_views(ray_start_regular):
    """Test that overlapping views of the same storage tensor are properly transferred."""

    actors = [OverlappingViewProducer.remote(), GPUTestActor.remote()]
    src_actor, dst_actor = actors[0], actors[1]

    refs = ray.get(src_actor.produce_overlapping_views.remote())
    result = ray.get(dst_actor.consume_with_nixl.remote(refs))
    assert result == 15


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class WaitTensorFreedActor:
    def test_wait_tensor_freed_views(self):
        from ray.experimental import wait_tensor_freed

        tensor = torch.tensor([1, 2, 3, 4, 5], dtype=torch.float32).to("cuda")
        slices = [tensor[0:3], tensor[1:4], tensor[2:5]]
        ref1 = ray.put(slices[0], _tensor_transport="nixl")
        ref2 = ray.put(slices[1], _tensor_transport="nixl")
        ref3 = ray.put(slices[2], _tensor_transport="nixl")
        del ref1
        wait_tensor_freed(slices[0], timeout=10)
        with pytest.raises(TimeoutError):
            wait_tensor_freed(slices[1], timeout=1)
        with pytest.raises(TimeoutError):
            wait_tensor_freed(slices[2], timeout=1)
        del ref2
        with pytest.raises(TimeoutError):
            wait_tensor_freed(slices[2], timeout=1)
        wait_tensor_freed(slices[1], timeout=10)
        del ref3
        wait_tensor_freed(slices[2], timeout=10)
        return "Success"


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_wait_tensor_freed_views(ray_start_regular):
    """Test that wait_tensor_freed tracks each view independently,
    not the shared underlying storage."""
    actor = WaitTensorFreedActor.remote()
    result = ray.get(actor.test_wait_tensor_freed_views.remote())
    assert result == "Success"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
