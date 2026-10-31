"""Axon transfer contract tests without native Axon or GPU dependencies."""

import asyncio
import gc
import sys
import unittest
import weakref
from concurrent.futures import Future
from dataclasses import dataclass
from unittest.mock import ANY, AsyncMock, Mock, call, patch

import pytest

from ray.experimental.rdt import axon_tensor_transport as adapter


@dataclass(frozen=True)
class Device:
    type: str
    index: int = 0


class Tensor:
    def __init__(self, shape=(4,), dtype="float32", device="cuda"):
        self.shape = shape
        self.dtype = dtype
        self.device = Device(device)
        self.copy_ = Mock()

    def is_contiguous(self):
        return True


def transport(cls=adapter.AxonCudaTensorTransport):
    manager = cls()
    manager._ensure_runtime = Mock()
    manager._worker_name = "source"
    manager._local_address = b"source-address"
    manager._runtime = Mock()
    manager._runtime.connect_endpoint_async = AsyncMock()
    manager._from_dlpack = Mock()
    manager._submit = asyncio.run
    return manager


def run_immediately(coro, loop):
    future = Future()
    future.set_result(asyncio.run(coro))
    return future


class CudaReceiveTests(unittest.TestCase):
    def setUp(self):
        self.torch = Mock()
        self.torch.cuda.current_device.return_value = 1
        self.torch.device.return_value = "cuda:1"
        self.patch = patch.dict("sys.modules", {"torch": self.torch})
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.source = transport()
        self.receiver = transport()
        self.communicator = self.receiver.get_communicator_metadata(None, None)

    def receive(self, tensors, results=None, targets=None):
        meta = self.source.extract_tensor_transport_metadata("object", tensors)
        self.receiver._runtime.invoke = AsyncMock(
            side_effect=tensors if results is None else results
        )
        return self.receiver.recv_multiple_tensors(
            "local-object", meta, self.communicator, targets
        )

    def test_receiver_fetches_each_tensor_in_order_without_a_sender_task(self):
        tensors = [Tensor(()), Tensor((65536,)), Tensor((8 * 1024 * 1024,))]
        self.assertTrue(self.source.is_one_sided())
        self.assertEqual(self.receive(tensors), tensors)
        self.receiver._runtime.connect_endpoint_async.assert_awaited_once_with(
            b"source-address", "source"
        )
        requests = self.receiver._runtime.invoke.await_args_list
        self.assertEqual(len(requests), 3)
        for index, request in enumerate(requests):
            self.assertEqual(request.args, ("object", index))
            self.assertEqual(
                request.kwargs["function"], adapter._AXON_FETCH_FUNCTION_ID
            )
            self.assertEqual(request.kwargs["worker_name"], "source")
            self.assertIs(request.kwargs["from_dlpack_fn"], self.receiver._from_dlpack)
            # Every response must carry the CUDA allocator, even for tiny tensors.
            self.assertIs(
                request.kwargs["memory_policy"]([]), self.torch.empty.return_value
            )
        self.assertEqual(
            self.torch.empty.call_args_list,
            [call(t.shape, dtype=t.dtype, device="cuda:1") for t in tensors],
        )
        with self.assertRaisesRegex(NotImplementedError, "recv_multiple_tensors"):
            self.source.send_multiple_tensors(tensors, None, self.communicator)

    def test_export_retains_tensors_until_garbage_collection(self):
        tensor = Tensor()
        ref = weakref.ref(tensor)
        meta = self.source.extract_tensor_transport_metadata("object", [tensor])
        self.assertEqual(
            (meta.source_worker, meta.source_address, meta.source_object_id),
            ("source", b"source-address", "object"),
        )
        del tensor
        gc.collect()
        self.assertIsNotNone(ref())
        self.source.garbage_collect("object", meta, [])
        gc.collect()
        self.assertIsNone(ref())
        self.source.garbage_collect("object", meta, [])

    def test_producer_gpu_work_finishes_before_export_is_published(self):
        tensors = [Tensor(), Tensor()]
        self.torch.cuda.synchronize.side_effect = lambda device: self.assertFalse(
            self.source._exports
        )
        self.source.extract_tensor_transport_metadata("object", tensors)
        self.torch.cuda.synchronize.assert_called_once_with(tensors[0].device)
        self.assertEqual(self.source._exports["object"], tuple(tensors))

    def test_fetch_service_returns_contiguous_tensor_and_propagates_missing_exports(
        self,
    ):
        tensors = [Tensor(), Tensor(dtype="int64")]
        meta = self.source.extract_tensor_transport_metadata("object", tensors)
        self.source._register_fetch_function(Tensor)
        handler, function = self.source._runtime.register_function.call_args.args
        self.assertEqual(function, adapter._AXON_FETCH_FUNCTION_ID)
        self.assertIs(handler.__annotations__["return"], Tensor)
        result = asyncio.run(handler("object", 1))
        self.assertIs(result, tensors[1])
        for index in (-1, 2):
            with self.assertRaisesRegex(ValueError, "tensor index"):
                asyncio.run(handler("object", index))
        self.source.garbage_collect("object", meta, tensors)
        self.assertIs(result, tensors[1])
        with self.assertRaises(KeyError):
            asyncio.run(handler("object", 0))

    def test_target_buffers_are_filled_after_fetch_completion(self):
        tensors = [Tensor(), Tensor(dtype="int64")]
        targets = [Tensor(), Tensor(dtype="int64")]
        self.assertIs(self.receive(tensors, targets=targets), targets)
        for target, source in zip(targets, tensors):
            target.copy_.assert_called_once_with(source)

    def test_failed_fetch_does_not_publish_partial_results_or_modify_targets(self):
        tensors, targets = [Tensor(), Tensor()], [Tensor(), Tensor()]
        with self.assertRaisesRegex(RuntimeError, "source unavailable"):
            self.receive(
                tensors, [tensors[0], RuntimeError("source unavailable")], targets
            )
        for target in targets:
            target.copy_.assert_not_called()

    def test_eager_host_response_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "host payload"):
            self.receive([Tensor()], [Tensor(device="cpu")])

    def test_empty_object_does_not_initialize_runtime(self):
        self.assertEqual(self.receive([]), [])
        self.source._ensure_runtime.assert_not_called()
        self.receiver._ensure_runtime.assert_not_called()
        self.receiver._runtime.invoke.assert_not_called()

    def test_zero_sized_tensor_keeps_its_position_without_an_rpc_payload(self):
        tensors = [Tensor((0, 3)), Tensor((), dtype="int64")]
        empty = Tensor((0, 3))
        self.torch.empty.return_value = empty
        result = self.receive(tensors, [tensors[1]])
        self.assertEqual(result, [empty, tensors[1]])
        self.assertEqual(
            self.torch.empty.call_args_list,
            [
                call((0, 3), dtype="float32", device="cuda:1"),
                call((), dtype="int64", device="cuda:1"),
            ],
        )
        self.assertEqual(self.receiver._runtime.invoke.await_args.args[-1], 1)

    def test_receive_policy_keeps_the_original_dtype_and_receiver_device(self):
        tensors = [Tensor((2, 3), dtype="bfloat16")]
        self.receive(tensors)
        policy = self.receiver._runtime.invoke.await_args.kwargs["memory_policy"]
        self.torch.cuda.current_device.return_value = 0
        self.assertIs(policy([]), self.torch.empty.return_value)
        self.torch.empty.assert_called_once_with(
            (2, 3), dtype="bfloat16", device="cuda:1"
        )

    def test_bad_target_buffers_fail_before_connecting_or_fetching(self):
        for targets in (
            [],
            [Tensor((3,))],
            [Tensor(dtype="int64")],
            [Tensor(device="cpu")],
        ):
            with self.subTest(targets=targets), self.assertRaisesRegex(
                ValueError, "Target buffer"
            ):
                self.receive([Tensor()], targets=targets)
        self.receiver._runtime.connect_endpoint_async.assert_not_called()
        self.receiver._runtime.invoke.assert_not_called()

    def test_wrong_source_device_is_rejected_before_publication(self):
        with self.assertRaisesRegex(ValueError, "requires cuda"):
            self.source.extract_tensor_transport_metadata(
                "object", [Tensor(device="cpu")]
            )
        self.assertFalse(self.source._exports)

    def test_runtime_thread_selects_the_callers_cuda_device(self):
        manager = adapter.AxonCudaTensorTransport()
        axon = Mock()
        self.torch.Tensor = Tensor
        with patch.dict("sys.modules", {"axon": axon}), patch.object(
            adapter, "_get_runtime_loop", return_value=(Mock(), Mock())
        ), patch.object(
            adapter.asyncio,
            "run_coroutine_threadsafe",
            side_effect=run_immediately,
        ):
            manager._ensure_runtime()
        self.torch.cuda.set_device.assert_called_once_with(1)
        axon.cuda.assert_called_once_with(device_id=1)
        self.assertTrue(manager._startup_future.done())
        axon.AxonRuntime.return_value.register_function.assert_called_once()


class CpuFetchRegressionTests(unittest.TestCase):
    def test_numpy_and_torch_use_one_public_fetch_path_for_all_sizes(self):
        import numpy as np
        import torch

        cases = (
            (
                adapter.AxonNumpyTensorTransport,
                lambda shape: np.ones(shape, dtype=np.int64),
            ),
            (
                adapter.AxonTorchTensorTransport,
                lambda shape: torch.ones(shape, dtype=torch.int64),
            ),
        )
        for cls, make_tensor in cases:
            source, receiver = transport(cls), transport(cls)
            for shape in ((), (1024,), (1024 * 1024,), (0, 3)):
                with self.subTest(backend=cls._backend, shape=shape):
                    tensor = make_tensor(shape)
                    meta = source.extract_tensor_transport_metadata("object", [tensor])
                    receiver._runtime.invoke = AsyncMock(return_value=tensor)
                    comm = receiver.get_communicator_metadata(None, None)
                    result = receiver.recv_multiple_tensors("object", meta, comm)
                    self.assertEqual(len(result), 1)
                    self.assertEqual(tuple(result[0].shape), shape)
                    self.assertEqual(result[0].dtype, tensor.dtype)
                    if 0 in shape:
                        receiver._runtime.invoke.assert_not_called()
                    else:
                        np.testing.assert_array_equal(result[0], tensor)
                        receiver._runtime.invoke.assert_awaited_once_with(
                            "object",
                            0,
                            worker_name="source",
                            session_id=0,
                            function=adapter._AXON_FETCH_FUNCTION_ID,
                            memory_policy=ANY,
                            from_dlpack_fn=receiver._from_dlpack,
                        )
                        policy = receiver._runtime.invoke.await_args.kwargs[
                            "memory_policy"
                        ]
                        self.assertIs(result[0], policy([]))
                        # Eager replies borrow UCX receive storage; returned
                        # tensors must not depend on that storage's lifetime.
                        tensor[...] = 2
                        np.testing.assert_array_equal(result[0], make_tensor(shape))
                        target = make_tensor(shape)
                        targets = [target]
                        self.assertIs(
                            receiver.recv_multiple_tensors(
                                "object", meta, comm, targets
                            ),
                            targets,
                        )
                        np.testing.assert_array_equal(target, tensor)
                    source.garbage_collect("object", meta, [])
            # Repeated objects reuse the connection, independently of their sizes.
            receiver._runtime.connect_endpoint_async.assert_awaited_once()

    def test_cpu_rendezvous_returns_the_supplied_receive_buffer(self):
        import numpy as np

        source = transport(adapter.AxonNumpyTensorTransport)
        receiver = transport(adapter.AxonNumpyTensorTransport)
        tensor = np.ones(16384, dtype=np.int64)
        meta = source.extract_tensor_transport_metadata("object", [tensor])
        allocated = []

        async def invoke(*args, memory_policy, **kwargs):
            buffer = memory_policy([])
            buffer[:] = tensor
            allocated.append(buffer)
            return buffer

        receiver._runtime.invoke = invoke
        with patch.object(np, "copyto", side_effect=AssertionError("Unexpected copy")):
            result = receiver.recv_multiple_tensors("object", meta, None)
        self.assertIs(result[0], allocated[0])
        np.testing.assert_array_equal(result[0], tensor)

    def test_noncontiguous_exports_are_rejected(self):
        import numpy as np
        import torch

        for cls, tensor in (
            (adapter.AxonNumpyTensorTransport, np.ones((4, 4))[:, ::2]),
            (adapter.AxonTorchTensorTransport, torch.ones((4, 4))[:, ::2]),
        ):
            manager = transport(cls)
            with self.assertRaisesRegex(ValueError, "contiguous"):
                manager.extract_tensor_transport_metadata("object", [tensor])
            self.assertFalse(manager._exports)


@pytest.fixture
def isolated_registry(monkeypatch):
    from ray.experimental.rdt import util

    monkeypatch.setattr(util, "transport_manager_info", {})
    monkeypatch.setattr(util, "transport_managers", {})
    monkeypatch.setattr(util, "_default_transports_registered", False)
    monkeypatch.setattr(util, "has_custom_transports", False)
    return util


@pytest.mark.parametrize(
    "backend,cls,device",
    [
        ("AXON", adapter.AxonNumpyTensorTransport, "cpu"),
        ("AXON_TORCH", adapter.AxonTorchTensorTransport, "cpu"),
        ("AXON_CUDA", adapter.AxonCudaTensorTransport, "cuda"),
    ],
)
def test_builtin_registration_is_lazy(
    isolated_registry, monkeypatch, backend, cls, device
):
    # Registering a backend must not import its optional native dependency.
    monkeypatch.setitem(sys.modules, "axon", None)
    util = isolated_registry
    assert util.normalize_and_validate_tensor_transport(backend.lower()) == backend
    assert util.device_match_transport(device, backend)
    assert not util.device_match_transport(
        "cuda" if device == "cpu" else "cpu", backend
    )
    manager = util.get_tensor_transport_manager(backend)
    assert isinstance(manager, cls)
    assert manager.tensor_transport_backend() == backend
    assert manager is util.get_tensor_transport_manager(backend)
    assert manager._runtime is None
    assert util.is_one_sided_transport(backend)
    assert not manager.can_abort_transport()
    assert not util.has_custom_transports
    # Existing built-ins remain available without Axon installed.
    assert util.normalize_and_validate_tensor_transport("gloo") == "GLOO"
    assert util.normalize_and_validate_tensor_transport("nixl") == "NIXL"


def test_numpy_registration_does_not_require_torch(isolated_registry, monkeypatch):
    import numpy as np

    monkeypatch.setitem(sys.modules, "torch", None)
    util = isolated_registry
    assert util.get_transport_data_type("AXON") is np.ndarray
    assert util.transport_manager_info["AXON"].devices == ["cpu"]
    assert "AXON_CUDA" not in util.transport_manager_info
    assert not util.has_custom_transports


@pytest.mark.parametrize("exc", [ImportError("no Axon"), RuntimeError("no CUDA")])
def test_availability_probe_handles_initialization_failure(
    isolated_registry, monkeypatch, exc
):
    manager = Mock()
    manager._ensure_runtime.side_effect = exc
    monkeypatch.setattr(
        isolated_registry, "get_tensor_transport_manager", lambda _: manager
    )
    assert not adapter._axon_transport_available_in_process("AXON")


def test_missing_native_dependency_has_actionable_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "axon", None)
    manager = adapter.AxonNumpyTensorTransport()
    try:
        with pytest.raises(ImportError, match="execution-ucx"):
            manager._ensure_runtime()
    finally:
        manager.shutdown()


def test_master_fetch_wait_interface_uses_receive_result():
    manager = transport(adapter.AxonNumpyTensorTransport)
    tensors = [object()]
    manager.recv_multiple_tensors = Mock(return_value=tensors)
    metadata = adapter.AxonTransportMetadata(tensor_meta=[])
    comm = adapter.CommunicatorMetadata()
    request = manager.fetch_multiple_tensors("object", metadata, comm)
    assert request.obj_id == "object"
    assert manager.wait_fetch_complete(request, timeout=0) is tensors
    manager.recv_multiple_tensors.assert_called_once_with(
        "object", metadata, comm, None
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
