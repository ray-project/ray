import sys

import pytest
import torch

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.experimental import set_target_for_ref
from ray.experimental.rdt.util import get_tensor_transport_manager


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class UCCLGPUTestActor:
    def __init__(self):
        self.reserved_tensor1 = torch.tensor([1, 2, 3]).to("cuda")
        self.reserved_tensor2 = torch.tensor([4, 5, 6]).to("cuda")
        self.reserved_tensor3 = torch.tensor([7, 8, 9]).to("cuda")

    @ray.method(tensor_transport="uccl")
    def echo(self, data, device):
        return data.to(device)

    def sum(self, data, device):
        assert data.device.type == device
        return data.sum().item()

    def produce(self, tensors):
        refs = []
        for t in tensors:
            refs.append(ray.put(t, _tensor_transport="uccl"))
        return refs

    def consume_with_uccl(self, refs):
        tensors = [ray.get(ref) for ref in refs]
        total = 0
        for t in tensors:
            assert t.device.type == "cuda"
            total += t.sum().item()
        return total

    def consume_with_object_store(self, refs):
        tensors = [ray.get(ref, _use_object_store=True) for ref in refs]
        total = 0
        for t in tensors:
            assert t.device.type == "cuda"
            total += t.sum().item()
        return total

    def gc(self):
        tensor = torch.tensor([1, 2, 3]).to("cuda")
        ref = ray.put(tensor, _tensor_transport="uccl")
        obj_id = ref.hex()
        rdt_manager = ray._private.worker.global_worker.rdt_manager
        uccl_transport = get_tensor_transport_manager("UCCL")

        assert rdt_manager.rdt_store.has_tensor(tensor)
        assert rdt_manager.is_managed_object(obj_id)
        assert obj_id in uccl_transport._managed_meta
        assert obj_id in uccl_transport._obj_descs

        del ref

        rdt_manager.rdt_store.wait_tensor_freed(tensor, timeout=10)
        assert not rdt_manager.rdt_store.has_tensor(tensor)
        assert not rdt_manager.is_managed_object(obj_id)
        assert obj_id not in uccl_transport._managed_meta
        assert obj_id not in uccl_transport._obj_descs
        return "Success"

    @ray.method(tensor_transport="uccl")
    def send_dict1(self):
        return {"round1-1": self.reserved_tensor1, "round1-2": self.reserved_tensor2}

    @ray.method(tensor_transport="uccl")
    def send_dict2(self):
        return {"round2-1": self.reserved_tensor1, "round2-3": self.reserved_tensor3}

    def sum_dict(self, dict):
        return sum(v.sum().item() for v in dict.values())

    def get_num_rdt_objects(self):
        rdt_manager = ray._private.worker.global_worker.rdt_manager
        return rdt_manager.rdt_store.get_num_objects()

    def get_num_managed_meta_uccl(self):
        return get_tensor_transport_manager("UCCL")._get_num_managed_meta()

    def put_shared_tensor_lists(self):
        """Create two tensor lists sharing a common tensor and put them with UCCL."""
        t1 = torch.tensor([1, 2, 3]).to("cuda")
        t2 = torch.tensor([4, 5, 6]).to("cuda")
        t3 = torch.tensor([7, 8, 9]).to("cuda")

        list1 = [t1, t2]
        list2 = [t2, t3]

        ref1 = ray.put(list1, _tensor_transport="uccl")
        # Each put registers its own descriptors; UCCL deduplicates overlapping
        # memory regions internally at the RDMA level (uccl_regmr MR cache).
        ref2 = ray.put(list2, _tensor_transport="uccl")

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
    actor = UCCLGPUTestActor.remote()
    tensor = torch.tensor([1, 2, 3]).to("cuda")
    ref1 = actor.echo.remote(tensor, "cuda")
    ref2 = actor.echo.remote(tensor, "cuda")
    ref3 = actor.echo.remote(tensor, "cuda")

    # Test ray.get with default tensor transport, should use uccl here.
    assert torch.equal(ray.get(ref1), tensor)

    # Test ray.get with uccl tensor transport
    assert torch.equal(ray.get(ref2), tensor)

    # Test ray.get with object store tensor transport
    assert torch.equal(ray.get(ref3, _use_object_store=True), tensor)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_p2p(ray_start_regular):
    num_actors = 2
    actors = [UCCLGPUTestActor.remote() for _ in range(num_actors)]

    src_actor, dst_actor = actors[0], actors[1]

    # Test GPU to GPU transfer
    tensor = torch.tensor([1, 2, 3])
    ref = src_actor.echo.remote(tensor, "cuda")
    result = dst_actor.sum.remote(ref, "cuda")
    assert tensor.sum().item() == ray.get(result)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_p2p_cpu_to_cpu(ray_start_regular):
    num_actors = 2
    actors = [UCCLGPUTestActor.remote() for _ in range(num_actors)]

    src_actor, dst_actor = actors[0], actors[1]

    # Test CPU to CPU transfer
    tensor1 = torch.tensor([4, 5, 6])
    ref1 = src_actor.echo.remote(tensor1, "cpu")
    result1 = dst_actor.sum.remote(ref1, "cpu")
    assert tensor1.sum().item() == ray.get(result1)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_intra_rdt_tensor_transfer(ray_start_regular):
    actor = UCCLGPUTestActor.remote()

    tensor = torch.tensor([1, 2, 3])

    # Intra-actor communication for pure GPU tensors
    ref = actor.echo.remote(tensor, "cuda")
    result = actor.sum.remote(ref, "cuda")
    assert tensor.sum().item() == ray.get(result)


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_put_and_get_object_with_uccl(ray_start_regular):
    actors = [UCCLGPUTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]
    tensor1 = torch.tensor([1, 2, 3]).to("cuda")
    tensor2 = torch.tensor([4, 5, 6, 0]).to("cuda")
    tensor3 = torch.tensor([7, 8, 9, 0, 0]).to("cuda")
    tensors = [tensor1, tensor2, tensor3]
    ref = src_actor.produce.remote(tensors)
    ref1 = dst_actor.consume_with_uccl.remote(ref)
    result1 = ray.get(ref1)
    assert result1 == 45


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_put_and_get_object_with_object_store(ray_start_regular):
    actors = [UCCLGPUTestActor.remote() for _ in range(2)]
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
    actor = UCCLGPUTestActor.remote()
    ref = actor.gc.remote()
    assert ray.get(ref) == "Success"


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_send_duplicate_tensor(ray_start_regular):
    actors = [UCCLGPUTestActor.remote() for _ in range(2)]
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
        lambda: ray.get(src_actor.get_num_managed_meta_uccl.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_uccl_abort_sender_dies_before_creating(ray_start_regular):
    actors = [UCCLGPUTestActor.remote() for _ in range(2)]

    # Trigger transfer and kill sender before the receiver starts receiving
    signal_actor = SignalActor.remote()
    actors[0].block_main_thread.remote(signal_actor)
    ref = actors[0].echo.remote(torch.randn((100, 100)), "cuda")
    result = actors[1].sum.remote(ref, "cuda")
    ray.kill(actors[0])

    with pytest.raises(ray.exceptions.ActorDiedError):
        ray.get(result)

    # Try a transfer with actor[1] receiving again
    new_actor = UCCLGPUTestActor.remote()
    ref = new_actor.echo.remote(torch.tensor([4, 5, 6]), "cuda")
    result = actors[1].sum.remote(ref, "cuda")
    assert ray.get(result) == 15


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_uccl_abort_sender_dies_before_sending(ray_start_regular):
    actors = [UCCLGPUTestActor.remote() for _ in range(2)]

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
    assert "UCCL" in exc_str and "The source actor may have died" in exc_str

    # Try a transfer with actor[1] receiving again
    new_actor = UCCLGPUTestActor.remote()
    ref = new_actor.echo.remote(torch.tensor([4, 5, 6]), "cuda")
    result = actors[1].sum.remote(ref, "cuda")
    assert ray.get(result) == 15


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_uccl_del_before_creating(ray_start_regular):
    """
    Blocking the main thread until we free the object from the reference counter.
    Then unblocking the actor's main thread so the object can be created and then
    asserting that the object was actually freed.
    """
    signal_actor = SignalActor.remote()
    actor = UCCLGPUTestActor.remote()
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
def test_uccl_owner_gets_from_launched_task(ray_start_regular):
    actor = UCCLGPUTestActor.remote()
    tensor = torch.randn((100, 100))

    ref = actor.echo.remote(tensor, "cuda")
    assert torch.equal(ray.get(ref), tensor.to("cuda"))


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_out_of_order_actors(ray_start_regular):
    @ray.remote(num_cpus=0, num_gpus=1, max_concurrency=10)
    class UCCLAsyncActor:
        def __init__(self):
            self.tensor = torch.tensor([4, 5, 6], device="cuda")

        @ray.method(tensor_transport="uccl")
        async def get_tensor(self):
            return self.tensor

        async def sum(self, data):
            return data.sum().item()

    actors = [UCCLAsyncActor.remote() for _ in range(2)]
    results = []
    for _ in range(100):
        ref = actors[0].get_tensor.remote()
        result = actors[1].sum.remote(ref)
        results.append(result)
    results = ray.get(results)
    assert sum(results) == 1500


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_shared_tensor_deduplication(ray_start_regular):
    """
    Test that tensors shared across multiple lists are properly handled.

    Creates list1 = [T1, T2] and list2 = [T2, T3] where T2 is shared.
    Each put registers its own descriptors; UCCL deduplicates the underlying
    RDMA memory region (MR) internally via its uccl_regmr MR cache.
    """
    actor = UCCLGPUTestActor.remote()
    ray.get(actor.put_shared_tensor_lists.remote())


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_uccl_endpoint_reuse(ray_start_regular):
    """
    We reuse UCCL remote endpoints by default. The receiver should successfully receive
    all tensors while the sender may trigger GC in between.
    """
    actors = [UCCLGPUTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]

    ref1 = src_actor.echo.remote(torch.tensor([1, 2, 3]).to("cuda"), "cuda")
    assert ray.get(dst_actor.sum.remote(ref1, "cuda")) == 6

    # Trigger another transfer. The receiver successfully gets
    # the latest tensor (UCCL endpoint connection is reused internally).
    ref2 = src_actor.echo.remote(torch.tensor([4, 5, 6]).to("cuda"), "cuda")
    assert ray.get(dst_actor.sum.remote(ref2, "cuda")) == 15

    del ref1, ref2

    # Wait for GC to free the tensors on the sender.
    wait_for_condition(
        lambda: ray.get(src_actor.get_num_managed_meta_uccl.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )

    # Transfer after GC. The receiver successfully gets
    # the latest tensor (UCCL endpoint is reconnected or reused internally).
    ref3 = src_actor.echo.remote(torch.tensor([7, 8, 9]).to("cuda"), "cuda")
    assert ray.get(dst_actor.sum.remote(ref3, "cuda")) == 24


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_uccl_endpoint_reuse_with_partial_tensors(ray_start_regular):
    """
    We reuse UCCL remote endpoints by default. The receiver should successfully choose
    and receive part of the tensors.
    """
    actors = [UCCLGPUTestActor.remote() for _ in range(2)]
    src_actor, dst_actor = actors[0], actors[1]

    ref1 = src_actor.echo.remote(torch.tensor([1, 2, 3, 4, 5, 6]).to("cuda"), "cuda")
    assert ray.get(dst_actor.sum.remote(ref1, "cuda")) == 21

    del ref1

    # Wait for GC to free the tensors on the sender.
    wait_for_condition(
        lambda: ray.get(src_actor.get_num_managed_meta_uccl.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )

    # Create the second tensor at the sender. The memory address of
    # this tensor may overlap with the first tensor (de-registered).
    ref2 = src_actor.echo.remote(torch.tensor([1, 2, 3]).to("cuda"), "cuda")

    # Create the third tensor at the sender. The memory address of
    # this tensor may overlap with the first tensor (de-registered).
    ref3 = src_actor.echo.remote(torch.tensor([4, 5, 6]).to("cuda"), "cuda")
    # Trigger the transfer. The receiver successfully gets the third tensor.
    assert ray.get(dst_actor.sum.remote(ref3, "cuda")) == 15

    del ref2, ref3


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_obj_descs_lifecycle(ray_start_regular):
    """Test that _obj_descs is populated on put and cleared on GC.

    Each ray.put gets its own entry in _obj_descs keyed by obj_id.
    UCCL handles MR deduplication internally; we just track descriptors
    per object so we can deregister them on garbage_collect.
    """
    from ray.experimental.rdt.uccl_tensor_transport import (
        UCCLTensorTransport,
    )

    transport = UCCLTensorTransport()

    tensor = torch.tensor([1, 2, 3, 4, 5, 6], dtype=torch.float32).to("cuda")

    # First put: one entry created.
    obj_id1 = "test_obj_id_1"
    meta1 = transport.extract_tensor_transport_metadata(obj_id1, [tensor])
    assert obj_id1 in transport._obj_descs
    assert len(transport._obj_descs[obj_id1]) == 1

    # Second put of the same tensor: a separate entry is created.
    # UCCL's internal MR cache deduplicates the underlying RDMA registration.
    obj_id2 = "test_obj_id_2"
    meta2 = transport.extract_tensor_transport_metadata(obj_id2, [tensor])
    assert obj_id2 in transport._obj_descs
    assert len(transport._obj_descs) == 2

    # GC of obj_id1 removes only its entry; obj_id2 remains.
    transport.garbage_collect(obj_id1, meta1, [tensor])
    assert obj_id1 not in transport._obj_descs
    assert obj_id2 in transport._obj_descs

    # GC of obj_id2 removes the last entry.
    transport.garbage_collect(obj_id2, meta2, [tensor])
    assert obj_id2 not in transport._obj_descs
    assert len(transport._obj_descs) == 0


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class UCCLOverlappingViewProducer:
    def produce_overlapping_views(self):
        tensor = torch.tensor([1, 2, 3, 4, 5], dtype=torch.float32).to("cuda")
        slices = [tensor[0:2], tensor[1:3], tensor[2:4]]
        refs = []
        for s in slices:
            refs.append(ray.put(s, _tensor_transport="uccl"))
        return refs


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_overlapping_views(ray_start_regular):
    """Test that overlapping views of the same storage tensor are properly transferred."""
    actors = [UCCLOverlappingViewProducer.remote(), UCCLGPUTestActor.remote()]
    src_actor, dst_actor = actors[0], actors[1]

    refs = ray.get(src_actor.produce_overlapping_views.remote())
    result = ray.get(dst_actor.consume_with_uccl.remote(refs))
    assert result == 15


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class UCCLWaitTensorFreedActor:
    def test_wait_tensor_freed_views(self):
        from ray.experimental import wait_tensor_freed

        tensor = torch.tensor([1, 2, 3, 4, 5], dtype=torch.float32).to("cuda")
        slices = [tensor[0:3], tensor[1:4], tensor[2:5]]
        ref1 = ray.put(slices[0], _tensor_transport="uccl")
        ref2 = ray.put(slices[1], _tensor_transport="uccl")
        ref3 = ray.put(slices[2], _tensor_transport="uccl")
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
    actor = UCCLWaitTensorFreedActor.remote()
    result = ray.get(actor.test_wait_tensor_freed_views.remote())
    assert result == "Success"


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 2}], indirect=True)
def test_uccl_get_into_tensor_buffers(ray_start_regular):
    @ray.remote(num_gpus=1, num_cpus=0)
    class UCCLBufferActor:
        def __init__(self):
            self.tensor_list = [
                torch.tensor([1, 2, 3]).to("cuda"),
                torch.tensor([4, 5, 6]).to("cuda"),
            ]

        def get_ref(self):
            return ray.put(self.tensor_list, _tensor_transport="uccl")

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

    actors = [UCCLBufferActor.remote() for _ in range(2)]
    ref = ray.get(actors[0].get_ref.remote())
    result = actors[1].get_with_buffers.remote([ref])
    assert ray.get(result)

    result = actors[1].get_with_wrong_buffers.remote([ref])
    assert ray.get(result)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
