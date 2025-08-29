import gc
import sys
import threading
import time

import pytest
import torch

import ray
from ray._private.custom_types import TensorTransportEnum
from ray.experimental.collective import create_collective_group
from ray.tests.test_gpu_objects_gloo import GPUTestActor

# tensordict is not supported on macos ci, so we skip the tests
support_tensordict = sys.platform != "darwin"

if support_tensordict:
    pass


def test_gpu_object_store_timeout(ray_start_regular):
    """Test the timeout mechanism of GPU object store"""
    gpu_object_store = ray.worker.global_worker.gpu_object_manager.gpu_object_store

    # Attempt to get a non-existent object and verify timeout exception
    non_existent_id = "non_existent_id"

    with pytest.raises(TimeoutError):
        gpu_object_store.wait_and_get_object(non_existent_id, timeout=0.5)

    with pytest.raises(TimeoutError):
        gpu_object_store.wait_and_pop_object(non_existent_id, timeout=0.5)


def test_unsupported_tensor_transport_backend(ray_start_regular):
    """Test handling of unsupported tensor transport backends"""
    from ray.experimental.gpu_object_manager.gpu_object_store import (
        _tensor_transport_to_collective_backend,
    )

    # Verify valid backend conversion
    valid_backend = _tensor_transport_to_collective_backend(TensorTransportEnum.GLOO)
    assert valid_backend == "torch_gloo"

    # Verify invalid backend handling
    class CustomEnum:
        name = "INVALID"

    with pytest.raises(ValueError, match="Invalid tensor transport"):
        _tensor_transport_to_collective_backend(CustomEnum())


def test_gpu_object_reference_leak(ray_start_regular):
    """Test GPU object reference recycling under failure scenarios (simulate real transport exceptions)"""
    world_size = 2
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="torch_gloo")

    sender = actors[0]
    receiver = actors[1]

    # Create GPU object and get reference
    tensor = torch.randn((10, 10))
    gpu_ref = sender.echo.remote(tensor)
    gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
    obj_id = gpu_ref.hex()

    # Initially add reference
    gpu_object_manager.add_gpu_object_ref(gpu_ref, sender, TensorTransportEnum.GLOO)

    # Verify metadata is registered
    with gpu_object_manager._metadata_lock:
        assert obj_id in gpu_object_manager.managed_gpu_object_metadata
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == 1

    # Simulate exception during transmission (more realistic failure scenario)
    try:
        # Trigger transmission (will increase reference count)
        gpu_object_manager.trigger_out_of_band_tensor_transfer(receiver, (gpu_ref,))
        # Throw exception immediately after transmission
        raise Exception("Simulate failure during tensor transfer")
    except Exception:
        pass

    # Actively delete reference and trigger GC
    del gpu_ref
    import gc

    gc.collect()
    time.sleep(1)  # Wait for weak reference callback execution

    # Verify metadata is completely cleaned up
    with gpu_object_manager._metadata_lock:
        assert obj_id not in gpu_object_manager.managed_gpu_object_metadata
        assert obj_id not in gpu_object_manager._gpu_object_ref_counts


def test_gpu_object_store_concurrent_access(ray_start_regular):
    """Test concurrent access safety of GPU object store"""
    gpu_object_store = ray.worker.global_worker.gpu_object_manager.gpu_object_store

    # Prepare test data
    num_tensors = 10
    tensors = [torch.randn((10, 10)) for _ in range(num_tensors)]
    obj_ids = [f"concurrent_test_id_{i}" for i in range(num_tensors)]

    # Thread coordination tools
    start_event = threading.Event()
    results = {"errors": 0}
    results_lock = threading.Lock()

    def add_objects(thread_id):
        start_event.wait()  # Wait for all threads to be ready
        try:
            idx = thread_id % num_tensors
            obj_id = obj_ids[idx]
            tensor = tensors[idx]

            # Concurrent object addition
            gpu_object_store.add_object(obj_id, [tensor], is_primary=True)
            # Concurrent object reading
            result = gpu_object_store.get_object(obj_id)
            assert torch.equal(result[0], tensor), f"Thread {thread_id} data mismatch"
            # Concurrent object deletion
            gpu_object_store.pop_object(obj_id)
        except Exception as e:
            print(f"Thread {thread_id} error: {e}")
            with results_lock:
                results["errors"] += 1

    # Start multi-threaded concurrent operations
    num_threads = 20
    threads = [
        threading.Thread(target=add_objects, args=(i,)) for i in range(num_threads)
    ]
    for t in threads:
        t.start()

    start_event.set()  # Trigger all threads to execute simultaneously
    for t in threads:
        t.join()

    # Verify no concurrent errors and objects are cleaned up
    assert results["errors"] == 0
    for obj_id in obj_ids:
        assert not gpu_object_store.has_object(obj_id)


def test_duplicate_gpu_object_ref_add(ray_start_regular):
    """Test reference count handling when adding the same GPU object reference repeatedly"""
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="torch_gloo")

    # Create test object
    tensor = torch.randn((10, 10))
    gpu_ref = actor.echo.remote(tensor)
    obj_id = gpu_ref.hex()
    gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager

    # First reference addition
    gpu_object_manager.add_gpu_object_ref(gpu_ref, actor, TensorTransportEnum.GLOO)
    with gpu_object_manager._metadata_lock:
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == 1

    # Add the same reference repeatedly (Expected: count should not reset, simulate possible code warnings)
    with pytest.warns(UserWarning, match="Duplicate add for GPU object ref"):
        gpu_object_manager.add_gpu_object_ref(gpu_ref, actor, TensorTransportEnum.GLOO)
    with gpu_object_manager._metadata_lock:
        assert (
            gpu_object_manager._gpu_object_ref_counts[obj_id] == 1
        )  # Ensure count is not overwritten

    # Actively increase count
    gpu_object_manager.increment_ref_count(obj_id)
    with gpu_object_manager._metadata_lock:
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == 2

    # Delete reference and trigger GC
    del gpu_ref
    import gc

    gc.collect()
    time.sleep(1)

    # Verify count is correctly decreased
    with gpu_object_manager._metadata_lock:
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == 1

    # Simulate final reference cleanup
    gpu_object_manager._cleanup_gpu_object_metadata(obj_id)
    with gpu_object_manager._metadata_lock:
        assert obj_id not in gpu_object_manager.managed_gpu_object_metadata
        assert obj_id not in gpu_object_manager._gpu_object_ref_counts


def test_gpu_object_manager_ref_counting(ray_start_regular):
    """Test reference count correctness in multi-receiver scenarios"""
    world_size = 3
    actors = [GPUTestActor.remote() for _ in range(world_size)]
    create_collective_group(actors, backend="torch_gloo")

    # Create object and get reference
    tensor = torch.randn((10, 10))
    sender = actors[0]
    receiver1 = actors[1]
    receiver2 = actors[2]
    gpu_ref = sender.echo.remote(tensor)
    obj_id = gpu_ref.hex()

    # Initialize reference management
    gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
    gpu_object_manager.add_gpu_object_ref(gpu_ref, sender, TensorTransportEnum.GLOO)
    initial_count = gpu_object_manager._gpu_object_ref_counts[obj_id]

    # Transmit to two receivers (count increases with each transmission)
    gpu_object_manager.trigger_out_of_band_tensor_transfer(receiver1, (gpu_ref,))
    gpu_object_manager.trigger_out_of_band_tensor_transfer(receiver2, (gpu_ref,))

    # Verify count increase
    with gpu_object_manager._metadata_lock:
        assert gpu_object_manager._gpu_object_ref_counts[obj_id] == initial_count + 2

    # Delete original reference
    del gpu_ref
    gc.collect()
    time.sleep(1)

    # Verify metadata still exists (because receivers hold references)
    with gpu_object_manager._metadata_lock:
        assert obj_id in gpu_object_manager.managed_gpu_object_metadata
        assert (
            gpu_object_manager._gpu_object_ref_counts[obj_id] == 2
        )  # Two remaining receiver references


def test_gpu_object_store_tensor_lifecycle(ray_start_regular):
    """Test complete lifecycle management of GPU objects"""
    actor = GPUTestActor.remote()
    create_collective_group([actor], backend="torch_gloo")

    # Manually manage object lifecycle
    tensor = torch.randn((100, 100))
    gpu_object_store = ray.worker.global_worker.gpu_object_manager.gpu_object_store
    obj_id = "test_tensor_lifecycle_id"

    # Add object
    gpu_object_store.add_object(obj_id, [tensor], is_primary=True)
    assert gpu_object_store.has_object(obj_id)
    assert gpu_object_store.is_primary_copy(obj_id)

    # Verify wait_tensor_freed timeout when not released
    with pytest.raises(TimeoutError):
        ray.experimental.wait_tensor_freed(tensor, timeout=0.5)

    # Background thread to delete object
    def remove_tensor():
        time.sleep(0.2)
        gpu_object_store.pop_object(obj_id)

    cleanup_thread = threading.Thread(target=remove_tensor)
    cleanup_thread.start()

    # Verify successful wait after object release
    ray.experimental.wait_tensor_freed(tensor)
    cleanup_thread.join()

    # Verify object is deleted
    assert not gpu_object_store.has_object(obj_id)


def test_gpu_object_manager_communicator_check(ray_start_regular):
    """Test communicator existence check"""
    actor = GPUTestActor.remote()
    gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager

    # Initially no communicator
    assert not gpu_object_manager.actor_has_tensor_transport(
        actor, TensorTransportEnum.GLOO
    )

    # Verify after creating communicator
    create_collective_group([actor], backend="torch_gloo")
    assert gpu_object_manager.actor_has_tensor_transport(
        actor, TensorTransportEnum.GLOO
    )

    # Verify other backends are still unavailable
    assert not gpu_object_manager.actor_has_tensor_transport(
        actor, TensorTransportEnum.NCCL
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
