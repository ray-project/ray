import multiprocessing.shared_memory as shm
import pickle
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy
import pytest

import ray
from ray._common.test_utils import wait_for_condition
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
        # Driver-created object IDs share a prefix. Let the OS choose a
        # short, unique name instead of truncating the object ID.
        shm_obj = shm.SharedMemory(create=True, size=size)
        shm_obj.buf[:size] = serialized_rdt_object
        self.shared_memory_objects[obj_id] = shm_obj

        return ShmTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device="cpu",
            shm_name=shm_obj.name,
            shm_size=size,
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


@pytest.fixture(scope="module")
def shared_memory_transport():
    register_tensor_transport(
        "shared_memory", ["cpu"], SharedMemoryTransport, numpy.ndarray
    )

    # Classes defined in test files get pickled by ref. So we need to
    # explicitly pickle the transport class in this module by value.
    # Note that this doesn't happen if you define the transport class on the
    # driver, something with pytest convinces cloudpickle to pickle by ref.
    from ray import cloudpickle

    cloudpickle.register_pickle_by_value(sys.modules[SharedMemoryTransport.__module__])


def test_register_and_use_custom_transport(ray_start_regular, shared_memory_transport):
    @ray.remote
    class Actor:
        @ray.method(tensor_transport="shared_memory")
        def echo(self, data):
            return data

        def non_rdt_echo(self, data):
            return data

        def sum(self, data):
            return data.sum().item()

    actors = [Actor.remote() for _ in range(2)]
    ref = actors[0].echo.remote(numpy.array([1, 2, 3]))
    result = actors[1].sum.remote(ref)
    assert ray.get(result) == 6

    # Test that non-rdt methods that return the data type still work.
    ref = actors[0].non_rdt_echo.remote(numpy.array([1, 2, 3]))
    result = actors[1].sum.remote(ref)
    assert ray.get(result) == 6


@pytest.mark.parametrize(
    "ray_start_regular",
    [
        {
            "include_dashboard": False,
            "_system_config": {
                "max_direct_call_object_size": 1024,
                "task_rpc_inlined_bytes_limit": 1024 * 1024,
            },
        },
        {
            "include_dashboard": False,
            "_system_config": {
                "max_direct_call_object_size": 1024 * 1024,
                "task_rpc_inlined_bytes_limit": 1024,
            },
        },
    ],
    indirect=True,
    ids=["object-size-limit", "rpc-size-limit"],
)
@pytest.mark.parametrize("source", ["task-return", "put", "driver-put"])
def test_large_rdt_payload_stays_in_memory(
    ray_start_regular, shared_memory_transport, source
):
    # Independently exceed the per-object and per-RPC inlining limits. The
    # tensor is small; it is the surrounding Python payload that is large.
    payload = b"x" * (128 * 1024)

    @ray.remote
    class Actor:
        def make_data(self):
            return {
                "tensor": numpy.array([1, 2, 3]),
                "payload": payload,
                "nested": ray.put("nested object"),
            }

        @ray.method(tensor_transport="shared_memory")
        def produce(self):
            return self.make_data()

        def put(self):
            return ray.put(self.make_data(), _tensor_transport="shared_memory")

        def hold(self, refs):
            self.ref = refs[0]

        def read(self):
            data = ray.get(self.ref)
            assert numpy.array_equal(data["tensor"], [1, 2, 3])
            assert data["payload"] == payload
            assert ray.get(data["nested"]) == "nested object"
            return ray._private.worker.global_worker.core_worker.object_exists(
                self.ref, memory_store_only=True
            )

        def release(self):
            del self.ref

        def num_rdt_objects(self):
            return (
                ray._private.worker.global_worker.rdt_manager.rdt_store.get_num_objects()
            )

        def plain_produce(self):
            return payload

        def plain_put(self):
            return ray.put(payload)

    sender, borrower = Actor.remote(), Actor.remote()
    # Plain actor methods and nested ObjectRefs do not trigger the usual
    # submission-time registration of a custom transport on the actors.
    manager = ray._private.worker.global_worker.rdt_manager
    for actor in (sender, borrower):
        manager.wait_until_custom_transports_registered(actor)
    if source == "driver-put":
        ref = ray.put(
            {
                "tensor": numpy.array([1, 2, 3]),
                "payload": payload,
                "nested": ray.put("nested object"),
            },
            _tensor_transport="shared_memory",
        )
    else:
        ref = (
            ray.get(sender.put.remote(), timeout=20)
            if source == "put"
            else sender.produce.remote()
        )

    def num_rdt_objects():
        if source == "driver-put":
            return manager.rdt_store.get_num_objects()
        return ray.get(sender.num_rdt_objects.remote())

    ready, _ = ray.wait([ref], timeout=20, fetch_local=False)
    assert ready == [ref]
    del ready
    core_worker = ray._private.worker.global_worker.core_worker
    assert core_worker.object_exists(ref, memory_store_only=True)
    data = ray.get(ref, timeout=20)
    assert numpy.array_equal(data["tensor"], [1, 2, 3])
    assert data["payload"] == payload
    assert ray.get(data["nested"]) == "nested object"
    del data

    # The borrower receives a nested RDT ObjectRef rather than an already
    # resolved argument. It must keep both the tensor and nested object alive
    # after the driver drops its Python reference.
    ray.get(borrower.hold.remote([ref]))
    del ref
    assert ray.get(borrower.read.remote())
    assert num_rdt_objects() == 1
    ray.get(borrower.release.remote())
    wait_for_condition(lambda: num_rdt_objects() == 0)

    # Ordinary task returns and puts must still use Plasma above these limits.
    plain_ref = (
        ray.get(sender.plain_put.remote())
        if source != "task-return"
        else sender.plain_produce.remote()
    )
    assert ray.get(plain_ref) == payload
    assert not core_worker.object_exists(plain_ref, memory_store_only=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
