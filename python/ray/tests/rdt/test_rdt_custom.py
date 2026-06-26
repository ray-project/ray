import multiprocessing.shared_memory as shm
import pickle
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy
import pytest

import ray
from ray.experimental import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
    register_tensor_transport,
)


@dataclass
class ShmTransportMetadata(TensorTransportMetadata):
    shm_name: Optional[str] = None
    shm_size: Optional[int] = None


@dataclass
class ShmCommunicatorMetadata(CommunicatorMetadata):
    pass


class SharedMemoryTransport(TensorTransportManager):
    def __init__(self):
        self.shared_memory_objects: Dict[str, shm.SharedMemory] = {}

    def tensor_transport_backend(self) -> str:
        return "shared_memory"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return False

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        return True

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List[numpy.ndarray],
    ) -> TensorTransportMetadata:

        tensor_meta = []
        if rdt_object:
            for tensor in rdt_object:
                tensor_meta.append((tensor.shape, tensor.dtype))

        serialized_rdt_object = pickle.dumps(rdt_object)
        size = len(serialized_rdt_object)
        # Shm name can't be as long as the obj_id, so we truncate it.
        name = obj_id[:20]
        shm_obj = shm.SharedMemory(name=name, create=True, size=size)
        shm_obj.buf[:size] = serialized_rdt_object
        self.shared_memory_objects[obj_id] = shm_obj

        return ShmTransportMetadata(
            tensor_meta=tensor_meta, tensor_device="cpu", shm_name=name, shm_size=size
        )

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> CommunicatorMetadata:
        return ShmCommunicatorMetadata()

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List[Any]] = None,
    ):
        shm_name = tensor_transport_metadata.shm_name
        size = tensor_transport_metadata.shm_size
        shm_block = shm.SharedMemory(name=shm_name)
        recv_tensors = pickle.loads(shm_block.buf[:size])
        shm_block.close()
        return recv_tensors

    def send_multiple_tensors(
        self,
        tensors: List[numpy.ndarray],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        pass

    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List[numpy.ndarray],
    ):
        self.shared_memory_objects[obj_id].close()
        self.shared_memory_objects[obj_id].unlink()
        del self.shared_memory_objects[obj_id]

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        pass


class SlowSharedMemoryTransport(SharedMemoryTransport):
    def tensor_transport_backend(self) -> str:
        return "slow_shared_memory"

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List[numpy.ndarray],
    ) -> TensorTransportMetadata:
        time.sleep(2)
        return super().extract_tensor_transport_metadata(obj_id, rdt_object)


class InvalidDeviceSharedMemoryTransport(SharedMemoryTransport):
    def tensor_transport_backend(self) -> str:
        return "invalid_device_shared_memory"

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List[numpy.ndarray],
    ) -> TensorTransportMetadata:
        tensor_meta = [(tensor.shape, tensor.dtype) for tensor in rdt_object]
        return ShmTransportMetadata(tensor_meta=tensor_meta, tensor_device="cuda")


def test_register_and_use_custom_transport(ray_start_regular):
    register_tensor_transport(
        "shared_memory", ["cpu"], SharedMemoryTransport, numpy.ndarray
    )

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="shared_memory")
        def echo(self, data):
            return data

        def non_rdt_echo(self, data):
            return data

        def sum(self, data):
            return data.sum().item()

    # Classes defined in test files get pickled by ref. So we need to
    # explicitly pickle the transport class in this module by value.
    # Note that this doesn't happen if you define the transport class on the
    # driver, something with pytest convinces cloudpickle to pickle by ref.
    from ray import cloudpickle

    cloudpickle.register_pickle_by_value(sys.modules[SharedMemoryTransport.__module__])

    actors = [Actor.remote() for _ in range(2)]
    ref = actors[0].echo.remote(numpy.array([1, 2, 3]))
    result = actors[1].sum.remote(ref)
    assert ray.get(result) == 6

    # Test that non-rdt methods that return the data type still work.
    ref = actors[0].non_rdt_echo.remote(numpy.array([1, 2, 3]))
    result = actors[1].sum.remote(ref)
    assert ray.get(result) == 6


def test_async_metadata_extraction_does_not_block_next_actor_task(ray_start_regular):
    register_tensor_transport(
        "slow_shared_memory", ["cpu"], SlowSharedMemoryTransport, numpy.ndarray
    )

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="slow_shared_memory")
        def slow_rdt_return(self):
            return numpy.array([1, 2, 3])

        def ping(self):
            return time.perf_counter()

    from ray import cloudpickle

    cloudpickle.register_pickle_by_value(
        sys.modules[SlowSharedMemoryTransport.__module__]
    )

    producer = Actor.remote()

    start = time.perf_counter()
    slow_ref = producer.slow_rdt_return.remote()
    ping_ref = producer.ping.remote()

    ping_time = ray.get(ping_ref, timeout=1.5)
    assert ping_time - start < 1.5
    assert numpy.array_equal(ray.get(slow_ref), numpy.array([1, 2, 3]))


def test_async_metadata_extraction_validates_tensor_device(ray_start_regular):
    register_tensor_transport(
        "invalid_device_shared_memory",
        ["cpu"],
        InvalidDeviceSharedMemoryTransport,
        numpy.ndarray,
    )

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="invalid_device_shared_memory")
        def invalid_device_return(self):
            return numpy.array([1, 2, 3])

    from ray import cloudpickle

    cloudpickle.register_pickle_by_value(
        sys.modules[InvalidDeviceSharedMemoryTransport.__module__]
    )

    actor = Actor.remote()
    with pytest.raises(ray.exceptions.RaySystemError, match="does not support"):
        ray.get(actor.invalid_device_return.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
