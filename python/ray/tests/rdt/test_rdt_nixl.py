import sys

import pytest
import torch

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.experimental import set_target_for_ref
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
        rdt_manager = ray._private.worker.global_worker.rdt_manager
        nixl_transport = get_tensor_transport_manager("NIXL")

        assert rdt_manager.rdt_store.has_tensor(tensor)
        assert rdt_manager.is_managed_object(obj_id)
        assert obj_id in nixl_transport._managed_meta_nixl
        # Tensor-level metadata counting: the tensor should have metadata_count=1
        key = tensor.untyped_storage().data_ptr()
        assert key in nixl_transport._tensor_desc_cache
        assert nixl_transport._tensor_desc_cache[key].metadata_count == 1

        del ref

        rdt_manager.rdt_store.wait_tensor_freed(tensor, timeout=10)
        assert not rdt_manager.rdt_store.has_tensor(tensor)
        assert not rdt_manager.is_managed_object(obj_id)
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

    def get_num_rdt_objects(self):
        rdt_manager = ray._private.worker.global_worker.rdt_manager
        return rdt_manager.rdt_store.get_num_objects()

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
def test_ray_get_rdt_ref_created_by_actor_task(ray_start_regular):
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
def test_intra_rdt_tensor_transfer(ray_start_regular):
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
        lambda: ray.get(src_actor.get_num_rdt_objects.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )
    wait_for_condition(
        lambda: ray.get(src_actor.get_num_managed_meta_nixl.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_abort_sender_dies_before_creating(ray_start_regular):
    actors = [GPUTestActor.remote() for _ in range(2)]

    # Trigger transfer and kill sender before the receiver starts receiving
    signal_actor = SignalActor.remote()
    actors[0].block_main_thread.remote(signal_actor)
    ref = actors[0].echo.remote(torch.randn((100, 100)), "cuda")
    result = actors[1].sum.remote(ref, "cuda")
    ray.kill(actors[0])

    with pytest.raises(ray.exceptions.ActorDiedError):
        ray.get(result)

    # Try a transfer with actor[1] receiving again
    new_actor = GPUTestActor.remote()
    ref = new_actor.echo.remote(torch.tensor([4, 5, 6]), "cuda")
    result = actors[1].sum.remote(ref, "cuda")
    assert ray.get(result) == 15


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
        lambda: ray._private.worker.global_worker.rdt_manager.get_rdt_metadata(obj_id)
        is None,
    )
    wait_for_condition(
        lambda: ray.get(actor.get_num_rdt_objects.remote()) == 0,
    )


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_owner_gets_from_launched_task(ray_start_regular):
    actor = GPUTestActor.remote()
    tensor = torch.randn((100, 100))

    ref = actor.echo.remote(tensor, "cuda")
    assert torch.equal(ray.get(ref), tensor.to("cuda"))


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_out_of_order_actors(ray_start_regular):
    @ray.remote(num_cpus=0, num_gpus=1, max_concurrency=10)
    class GPUTestActor:
        def __init__(self):
            self.tensor = torch.tensor([4, 5, 6], device="cuda")

        @ray.method(tensor_transport="nixl")
        async def get_tensor(self):
            return self.tensor

        async def sum(self, data):
            return data.sum().item()

    actors = [GPUTestActor.remote() for _ in range(2)]
    results = []
    for _ in range(100):
        ref = actors[0].get_tensor.remote()
        result = actors[1].sum.remote(ref)
        results.append(result)
    results = ray.get(results)
    assert sum(results) == 1500


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


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_get_into_tensor_buffers(ray_start_regular):
    @ray.remote(num_gpus=1, num_cpus=0)
    class GPUTestActor:
        def __init__(self):
            self.tensor_list = [
                torch.tensor([1, 2, 3]).to("cuda"),
                torch.tensor([4, 5, 6]).to("cuda"),
            ]

        def get_ref(self):
            return ray.put(self.tensor_list, _tensor_transport="nixl")

        def get_with_buffers(self, refs):
            set_target_for_ref(refs[0], self.tensor_list)
            tensors = ray.get(refs[0])
            # Make sure we ray.get-ted into the buffers
            for new_tensor, tensor_buffer in zip(tensors, self.tensor_list):
                assert id(new_tensor) == id(tensor_buffer)
            return True

        def get_with_wrong_buffers(self, refs):
            wrong_tensor_buffer = [
                torch.tensor([1, 2]).to("cuda"),
                torch.tensor([4, 5]).to("cuda"),
            ]
            set_target_for_ref(refs[0], wrong_tensor_buffer)
            with pytest.raises(ValueError) as excinfo:
                ray.get(refs[0])
            assert "Shape of tensor_buffer at index 0" in str(excinfo.value)
            return True

    actors = [GPUTestActor.remote() for _ in range(2)]
    ref = ray.get(actors[0].get_ref.remote())
    result = actors[1].get_with_buffers.remote([ref])
    assert ray.get(result)

    result = actors[1].get_with_wrong_buffers.remote([ref])
    assert ray.get(result)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_register_deregister_nixl_memory(ray_start_regular):
    """
    Test that register_nixl_memory persists the NIXL memory registration when the object ref goes out of scope
    """
    from ray.experimental.rdt.nixl_tensor_transport import (
        NixlTensorTransport,
    )

    transport = NixlTensorTransport()
    tensor = torch.tensor([1, 2, 3]).to("cuda")

    transport.register_nixl_memory(tensor)
    key = tensor.untyped_storage().data_ptr()
    assert key in transport._tensor_desc_cache
    assert transport._tensor_desc_cache[key].metadata_count == 1

    # Simulate ray.put via extract_tensor_transport_metadata and bump the reference count
    obj_id = "test_obj_id"
    meta = transport.extract_tensor_transport_metadata(obj_id, [tensor])
    assert transport._tensor_desc_cache[key].metadata_count == 2

    # Simulate GC via garbage_collect and decrement the reference count
    transport.garbage_collect(obj_id, meta, [tensor])
    assert key in transport._tensor_desc_cache
    # The reference count should be 1 due to being bumped by register_nixl_memory
    assert transport._tensor_desc_cache[key].metadata_count == 1

    # decrement the remaining count to 0 and deregister the memory
    transport.deregister_nixl_memory(tensor)
    assert key not in transport._tensor_desc_cache


@pytest.mark.parametrize("device", ["cpu", "cuda"])
@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_memory_pool(ray_start_regular, device):
    """
    Test NIXL memory pool: use the pre-allocated memory pool for NIXL transfers when available.
    When the pool cannot accommodate an allocation, an error is raised.
    """

    @ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
    class PoolActor:
        def __init__(self, pool_device, pool_size):
            from ray.experimental import register_nixl_memory_pool

            register_nixl_memory_pool(pool_size, torch.device(pool_device))

        @ray.method(tensor_transport="nixl")
        def echo(self, data, device):
            return data.to(device)

        def get_num_managed_meta_nixl(self):
            return get_tensor_transport_manager("NIXL")._get_num_managed_meta_nixl()

    # int64 tensors of 3 elems = 24 bytes; with 16-byte alignment each carves 32.
    # Pool of 64 fits exactly two such tensors.
    src_actor = PoolActor.remote(device, 64)
    dst_actor = GPUTestActor.remote()

    # Transfer the first small tensor (using memory pool internally).
    ref1 = src_actor.echo.remote(torch.tensor([1, 2, 3]).to(device), device)
    assert ray.get(dst_actor.sum.remote(ref1, device)) == 6

    # Transfer the second small tensor (using memory pool internally).
    ref2 = src_actor.echo.remote(torch.tensor([4, 5, 6]).to(device), device)
    assert ray.get(dst_actor.sum.remote(ref2, device)) == 15

    # Third transfer: pool is full. The allocation raises
    # NixlOutOfMemoryError, which surfaces as a RayTaskError.
    ref3 = src_actor.echo.remote(torch.tensor([7, 8, 9]).to(device), device)
    with pytest.raises(ray.exceptions.RayTaskError) as excinfo:
        ray.get(dst_actor.sum.remote(ref3, device))
    assert "NixlOutOfMemoryError" in str(excinfo.value) and "out of memory" in str(
        excinfo.value
    )

    del ref1, ref2, ref3

    # Wait for GC to free the tensors on the sender.
    wait_for_condition(
        lambda: ray.get(src_actor.get_num_managed_meta_nixl.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )

    # Transfer the fourth tensor (after GC, using memory pool internally).
    ref4 = src_actor.echo.remote(torch.tensor([1, 2, 3, 4, 5, 6]).to(device), device)
    assert ray.get(dst_actor.sum.remote(ref4, device)) == 21


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_nixl_memory_pool_copies_tensor_not_storage(ray_start_regular):
    """Pool copies only each tensor's own bytes, not the full backing storage."""
    from ray.experimental.rdt.nixl_tensor_transport import (
        NixlTensorTransport,
    )

    transport = NixlTensorTransport()
    base = torch.arange(1000, dtype=torch.float32).to("cuda")
    view = base[100:104]  # 16 bytes; full storage is 4000 bytes
    view_bytes = view.numel() * view.element_size()

    # Pool sized for the view only — full storage would not fit.
    transport.register_nixl_memory_pool(64, torch.device("cuda"))

    obj_id = "view_copy_obj"
    meta = transport.extract_tensor_transport_metadata(obj_id, [view])
    assert obj_id in transport._memory_pool._allocated_by_obj
    blocks = transport._memory_pool._allocated_by_obj[obj_id]
    assert len(blocks) == 1
    assert blocks[0].size >= view_bytes
    assert blocks[0].size < base.untyped_storage().nbytes()

    # One transfer descriptor for the single packed block.
    descs = transport.get_nixl_agent().deserialize_descs(meta.nixl_serialized_descs)
    assert descs.descCount() == 1
    assert descs[0][1] == view_bytes

    transport.garbage_collect(obj_id, meta, [view])
    assert obj_id not in transport._memory_pool._allocated_by_obj


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_nixl_memory_pool_collapses_descriptors(ray_start_regular):
    """Multiple tensors in one put collapse to a single descriptor when unfragmented."""
    from ray.experimental.rdt.nixl_tensor_transport import (
        NixlTensorTransport,
    )

    transport = NixlTensorTransport()
    t0 = torch.arange(4, dtype=torch.float32).to("cuda")
    t1 = torch.arange(4, dtype=torch.float32).to("cuda") + 10
    t2 = torch.arange(4, dtype=torch.float32).to("cuda") + 20
    total = sum(t.numel() * t.element_size() for t in [t0, t1, t2])

    transport.register_nixl_memory_pool(1024, torch.device("cuda"))
    obj_id = "collapse_obj"
    meta = transport.extract_tensor_transport_metadata(obj_id, [t0, t1, t2])

    descs = transport.get_nixl_agent().deserialize_descs(meta.nixl_serialized_descs)
    assert descs.descCount() == 1
    # A group sharing one dtype packs with no padding.
    assert descs[0][1] == total
    assert len(transport._memory_pool._allocated_by_obj[obj_id]) == 1

    transport.garbage_collect(obj_id, meta, [t0, t1, t2])


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_nixl_memory_pool_fragmented_multi_descriptor(ray_start_regular):
    """Fragmented free list yields multiple descriptors with correct data."""
    from ray.experimental.rdt.nixl_memory_pool import (
        TensorLayout,
        group_tensors_by_desc,
    )
    from ray.experimental.rdt.nixl_tensor_transport import (
        NixlTensorTransport,
    )

    transport = NixlTensorTransport()
    # Three 32-byte fillers fill a 96-byte pool; free the outer two.
    transport.register_nixl_memory_pool(96, torch.device("cuda"))
    fillers = [torch.zeros(8, dtype=torch.float32).to("cuda") for _ in range(3)]
    filler_metas = []
    for i, f in enumerate(fillers):
        mid = transport.extract_tensor_transport_metadata(f"filler_{i}", [f])
        filler_metas.append(mid)
    # Free outer holes.
    transport.garbage_collect("filler_0", filler_metas[0], [fillers[0]])
    transport.garbage_collect("filler_2", filler_metas[2], [fillers[2]])

    t0 = torch.arange(8, dtype=torch.float32).to("cuda")
    t1 = torch.arange(8, dtype=torch.float32).to("cuda") + 10
    obj_id = "frag_obj"
    meta = transport.extract_tensor_transport_metadata(obj_id, [t0, t1])

    descs = transport.get_nixl_agent().deserialize_descs(meta.nixl_serialized_descs)
    assert descs.descCount() == 2
    assert len(transport._memory_pool._allocated_by_obj[obj_id]) == 2

    # Neither hole fits both tensors, so the receiver recovers one tensor per
    # descriptor from the lengths alone.
    layouts = [
        TensorLayout(t.numel() * t.element_size(), t.element_size()) for t in (t0, t1)
    ]
    packed_group_nbytes = [descs[i][1] for i in range(descs.descCount())]
    assert group_tensors_by_desc(layouts, packed_group_nbytes) == [[0], [1]]

    transport.garbage_collect(obj_id, meta, [t0, t1])
    transport.garbage_collect("filler_1", filler_metas[1], [fillers[1]])


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_memory_pool_with_target_buffers(ray_start_regular):
    """Pool-backed multi-tensor put works with set_target_for_ref (split path)."""

    @ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
    class PoolSrc:
        def __init__(self):
            from ray.experimental import register_nixl_memory_pool

            register_nixl_memory_pool(1024, torch.device("cuda"))
            self.tensors = [
                torch.tensor([1.0, 2.0, 3.0], dtype=torch.float32).to("cuda"),
                torch.tensor([4.0, 5.0, 6.0], dtype=torch.float32).to("cuda"),
            ]

        def get_ref(self):
            return ray.put(self.tensors, _tensor_transport="nixl")

    @ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
    class PoolDst:
        def __init__(self):
            self.buffers = [
                torch.empty(3, dtype=torch.float32, device="cuda"),
                torch.empty(3, dtype=torch.float32, device="cuda"),
            ]

        def get_with_buffers(self, refs):
            from ray.util.debug import log_once

            set_target_for_ref(refs[0], self.buffers)
            tensors = ray.get(refs[0])
            for t, buf in zip(tensors, self.buffers):
                assert id(t) == id(buf)
            # Separately allocated buffers cannot take the sender's packed
            # region in one read, so the receive path splits the group back
            # into one descriptor per tensor and logs that it did so.
            assert not log_once("nixl_target_buffers_not_packed")
            return [t.cpu().tolist() for t in tensors]

    src = PoolSrc.remote()
    dst = PoolDst.remote()
    ref = ray.get(src.get_ref.remote())
    result = ray.get(dst.get_with_buffers.remote([ref]))
    assert result == [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_memory_pool_contiguous_target_buffers(ray_start_regular):
    """Views of one pre-allocated buffer match the packed layout (merged path)."""

    @ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
    class PoolSrc:
        def __init__(self):
            from ray.experimental import register_nixl_memory_pool

            register_nixl_memory_pool(1024, torch.device("cuda"))
            self.tensors = [
                torch.tensor([1.0, 2.0, 3.0, 4.0], dtype=torch.float32).to("cuda"),
                torch.tensor([5.0, 6.0, 7.0, 8.0], dtype=torch.float32).to("cuda"),
            ]

        def get_ref(self):
            return ray.put(self.tensors, _tensor_transport="nixl")

    @ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
    class PoolDst:
        def __init__(self):
            # One pre-allocated buffer carved into per-tensor views, which is
            # the layout the sender packs into.
            self.parent = torch.empty(8, dtype=torch.float32, device="cuda")
            self.buffers = [self.parent[0:4], self.parent[4:8]]

        def get_with_buffers(self, refs):
            from ray.util.debug import log_once

            # The ref must be passed inside a list, otherwise Ray dereferences
            # the argument and transfers the object before this method runs.
            set_target_for_ref(refs[0], self.buffers)
            tensors = ray.get(refs[0])
            for t, buf in zip(tensors, self.buffers):
                assert t.data_ptr() == buf.data_ptr()
            # The buffers already match the sender's packed layout, so the whole
            # region arrives in one read: the fallback to one descriptor per
            # tensor never ran and so never claimed this log key.
            assert log_once("nixl_target_buffers_not_packed")
            return self.parent.cpu().tolist()

    src = PoolSrc.remote()
    dst = PoolDst.remote()
    ref = ray.get(src.get_ref.remote())
    assert ray.get(dst.get_with_buffers.remote([ref])) == [
        1.0,
        2.0,
        3.0,
        4.0,
        5.0,
        6.0,
        7.0,
        8.0,
    ]


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_nixl_memory_pool_multi_tensor_e2e(ray_start_regular):
    """End-to-end multi-tensor pool put collapses to one descriptor and transfers."""

    @ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
    class PoolSrc:
        def __init__(self):
            from ray.experimental import register_nixl_memory_pool

            register_nixl_memory_pool(1024, torch.device("cuda"))

        def put_list(self):
            tensors = [
                torch.tensor([1.0, 2.0], dtype=torch.float32).to("cuda"),
                torch.tensor([3.0, 4.0, 5.0], dtype=torch.float32).to("cuda"),
            ]
            ref = ray.put(tensors, _tensor_transport="nixl")
            meta = get_tensor_transport_manager("NIXL")._get_meta(ref.hex())
            descs = (
                get_tensor_transport_manager("NIXL")
                .get_nixl_agent()
                .deserialize_descs(meta.nixl_serialized_descs)
            )
            return ref, descs.descCount()

    @ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
    class PoolDst:
        def consume(self, refs):
            # The ref must be passed inside a list, otherwise Ray dereferences
            # the argument and transfers the object before this method runs.
            tensors = ray.get(refs[0])
            return [t.cpu().tolist() for t in tensors]

    src = PoolSrc.remote()
    dst = PoolDst.remote()
    ref, desc_count = ray.get(src.put_list.remote())
    assert desc_count == 1
    result = ray.get(dst.consume.remote([ref]))
    assert result == [[1.0, 2.0], [3.0, 4.0, 5.0]]


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_set_nixl_cuda_stream(ray_start_regular):
    """set_nixl_cuda_stream restricts the pre-registration sync to the given
    stream, and the transferred data is still valid."""

    @ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
    class StreamActor:
        @ray.method(tensor_transport="nixl")
        def echo_on_stream(self, data):
            from ray.experimental import set_nixl_cuda_stream

            stream = torch.cuda.Stream()
            with torch.cuda.stream(stream):
                out = data.to("cuda") * 2
            # Only block on `stream` instead of every stream on the device.
            set_nixl_cuda_stream(stream)
            return out

        @ray.method(tensor_transport="nixl")
        def echo_default(self, data):
            from ray.experimental import set_nixl_cuda_stream

            # Reset to the default full-device synchronization.
            set_nixl_cuda_stream(None)
            return data.to("cuda") * 2

    src_actor = StreamActor.remote()
    dst_actor = GPUTestActor.remote()

    ref = src_actor.echo_on_stream.remote(torch.tensor([1, 2, 3]))
    assert ray.get(dst_actor.sum.remote(ref, "cuda")) == 12

    # After resetting to None, transfers still succeed.
    ref2 = src_actor.echo_default.remote(torch.tensor([4, 5, 6]))
    assert ray.get(dst_actor.sum.remote(ref2, "cuda")) == 30


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_set_nixl_cuda_stream_overwrite(ray_start_regular):
    """Setting a stream overwrites any previous stream, and passing None
    clears it."""
    from ray.experimental.rdt.nixl_tensor_transport import (
        NixlTensorTransport,
    )

    transport = NixlTensorTransport()
    assert transport._cuda_stream is None

    stream1 = torch.cuda.Stream()
    stream2 = torch.cuda.Stream()
    transport.set_cuda_stream(stream1)
    assert transport._cuda_stream is stream1

    # Setting again overwrites the previous stream.
    transport.set_cuda_stream(stream2)
    assert transport._cuda_stream is stream2

    # None clears the recorded stream.
    transport.set_cuda_stream(None)
    assert transport._cuda_stream is None


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_set_nixl_cuda_stream_uncovered_device(ray_start_regular):
    """A device used by the RDT object with no matching stream errors."""
    from ray.experimental.rdt.nixl_tensor_transport import (
        NixlTensorTransport,
    )

    transport = NixlTensorTransport()
    # Provide a stream only for cuda:1, but create the tensor on cuda:0.
    stream = torch.cuda.Stream(device=torch.device("cuda:1"))
    transport.set_cuda_stream(stream)

    tensor = torch.tensor([1, 2, 3]).to("cuda:0")
    with pytest.raises(ValueError, match="Device mismatch between the CUDA stream"):
        transport.extract_tensor_transport_metadata("uncovered_obj", [tensor])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
