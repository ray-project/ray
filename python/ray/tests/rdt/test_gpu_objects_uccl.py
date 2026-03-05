import sys

import pytest
import torch

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
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

    def gc(self):
        tensor = torch.tensor([1, 2, 3]).to("cuda")
        ref = ray.put(tensor, _tensor_transport="uccl")
        obj_id = ref.hex()
        gpu_manager = ray._private.worker.global_worker.gpu_object_manager
        uccl_transport = get_tensor_transport_manager("UCCL")

        assert gpu_manager.gpu_object_store.has_tensor(tensor)
        assert gpu_manager.is_managed_object(obj_id)
        assert obj_id in uccl_transport._managed_meta
        key = tensor.data_ptr()
        assert key in uccl_transport._tensor_desc_cache
        assert uccl_transport._tensor_desc_cache[key].metadata_count == 1

        del ref

        gpu_manager.gpu_object_store.wait_tensor_freed(tensor, timeout=10)
        assert not gpu_manager.gpu_object_store.has_tensor(tensor)
        assert not gpu_manager.is_managed_object(obj_id)
        assert obj_id not in uccl_transport._managed_meta
        assert key not in uccl_transport._tensor_desc_cache
        return "Success"

    @ray.method(tensor_transport="uccl")
    def send_dict1(self):
        return {"round1-1": self.reserved_tensor1, "round1-2": self.reserved_tensor2}

    @ray.method(tensor_transport="uccl")
    def send_dict2(self):
        return {"round2-1": self.reserved_tensor1, "round2-3": self.reserved_tensor3}

    def sum_dict(self, dict):
        return sum(v.sum().item() for v in dict.values())

    def get_num_gpu_objects(self):
        gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
        return gpu_object_manager.gpu_object_store.get_num_objects()

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
        # UCCL deduplicates by data_ptr, so t2 is only registered once.
        ref2 = ray.put(list2, _tensor_transport="uccl")

        return ref1, ref2

    @ray.method(concurrency_group="_ray_system")
    def block_background_thread(self, signal_actor):
        ray.get(signal_actor.wait.remote())

    def block_main_thread(self, signal_actor):
        ray.get(signal_actor.wait.remote())


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_ray_get_gpu_ref_created_by_actor_task(ray_start_regular):
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
def test_intra_gpu_tensor_transfer(ray_start_regular):
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
        lambda: ray.get(src_actor.get_num_gpu_objects.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )
    wait_for_condition(
        lambda: ray.get(src_actor.get_num_managed_meta_uccl.remote()) == 0,
        timeout=10,
        retry_interval_ms=100,
    )


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
        lambda: ray._private.worker.global_worker.gpu_object_manager.get_gpu_object_metadata(
            obj_id
        )
        is None,
    )
    wait_for_condition(
        lambda: ray.get(actor.get_num_gpu_objects.remote()) == 0,
    )


@pytest.mark.parametrize("ray_start_regular", [{"num_gpus": 1}], indirect=True)
def test_shared_tensor_deduplication(ray_start_regular):
    """
    Test that tensors shared across multiple lists are properly deduplicated.

    Creates list1 = [T1, T2] and list2 = [T2, T3] where T2 is shared.
    UCCL deduplicates by data_ptr, so T2 is only registered once across both puts.
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
def test_data_ptr_level_reference_count(ray_start_regular):
    """Test that UCCL uses data_ptr() as the cache key for tensor deduplication.

    Registering the same tensor under two object IDs increments
    the metadata_count. When each obj ref goes out of scope via garbage_collect,
    the count decrements. After both are freed, the cache entry is removed.
    """
    from ray.experimental.rdt.uccl_tensor_transport import (
        UCCLTensorTransport,
    )

    transport = UCCLTensorTransport()

    tensor = torch.tensor([1, 2, 3, 4, 5, 6], dtype=torch.float32).to("cuda")
    data_ptr = tensor.data_ptr()

    # Simulate ray.put(tensor) - first registration
    obj_id1 = "test_obj_id_1"
    meta1 = transport.extract_tensor_transport_metadata(obj_id1, [tensor])
    assert len(transport._tensor_desc_cache) == 1
    assert transport._tensor_desc_cache[data_ptr].metadata_count == 1

    # Simulate ray.put(tensor) again with the same tensor object (same data_ptr).
    # The cache entry is reused and metadata_count is incremented.
    obj_id2 = "test_obj_id_2"
    meta2 = transport.extract_tensor_transport_metadata(obj_id2, [tensor])
    assert len(transport._tensor_desc_cache) == 1
    assert transport._tensor_desc_cache[data_ptr].metadata_count == 2

    # Simulate the obj ref for obj_id1 going out of scope.
    # The cache entry should still exist since obj_id2 is still alive.
    transport.garbage_collect(obj_id1, meta1, [tensor])
    assert data_ptr in transport._tensor_desc_cache
    assert transport._tensor_desc_cache[data_ptr].metadata_count == 1

    # Simulate the obj ref for obj_id2 going out of scope.
    # The cache entry should now be removed.
    transport.garbage_collect(obj_id2, meta2, [tensor])
    assert data_ptr not in transport._tensor_desc_cache


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
