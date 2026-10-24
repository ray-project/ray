"""Native Axon integration tests. Install execution-ucx before running these.

Set RAY_PYTEST_USE_GPU=1 to include CUDA transfers on a machine with two GPUs.
"""

import gc
import importlib.util
import json
import os
import subprocess
import sys

import numpy as np
import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("axon") is None, reason="execution-ucx is not installed"
)


def _tensor_actor():
    def _make_tensors(backend):
        tensors = [
            np.array(7, dtype=np.float32),
            np.empty((0, 3), dtype=np.float64),
            np.arange(16384, dtype=np.int64).reshape(128, 128),
            np.arange(2 * 1024 * 1024, dtype=np.float32),
        ]
        if backend == "AXON":
            return tensors
        import torch

        device = "cuda" if backend == "AXON_CUDA" else "cpu"
        return [torch.from_numpy(tensor).to(device) for tensor in tensors]

    def _check_tensors(tensors, backend):
        expected = _make_tensors(backend)
        assert len(tensors) == len(expected)
        for actual, reference in zip(tensors, expected):
            assert type(actual) is type(reference)
            assert actual.shape == reference.shape
            assert actual.dtype == reference.dtype
            if backend == "AXON":
                np.testing.assert_array_equal(actual, reference)
            else:
                import torch

                assert actual.device == reference.device
                assert torch.equal(actual, reference)

    @ray.remote(enable_tensor_transport=True)
    class TensorActor:
        def produce(self, backend):
            if backend == "AXON_CUDA":
                import torch

                # Publish tensors written on a non-default producer stream.
                with torch.cuda.stream(torch.cuda.Stream()):
                    return {"tensors": _make_tensors(backend)}
            return {"tensors": _make_tensors(backend)}

        def consume(self, value, backend):
            _check_tensors(value["tensors"], backend)
            return True

        def put(self, backend):
            return [ray.put(_make_tensors(backend), _tensor_transport=backend)]

        def busy(self, ready, release):
            ray.get(ready.send.remote())
            ray.get(release.wait.remote())

        def runtime_state(self):
            from ray.experimental.rdt import axon_tensor_transport as adapter
            from ray.experimental.rdt.util import transport_managers

            return {
                "active": sorted(
                    name
                    for name, manager in transport_managers.items()
                    if name.startswith("AXON") and manager._runtime is not None
                ),
                "owners": len(adapter._runtime_users),
                "threads": int(adapter._runtime_loop_thread is not None),
            }

        def close_transport(self, backend):
            from ray.experimental.rdt.util import (
                transport_managers,
                transport_managers_lock,
            )

            transport_managers[backend].shutdown()
            with transport_managers_lock:
                del transport_managers[backend]

    return TensorActor, _check_tensors


TensorActor, _check_tensors = _tensor_actor()


def _configure_axon_environment(use_cuda, monkeypatch):
    if use_cuda:
        if os.environ.get("RAY_PYTEST_USE_GPU") != "1":
            pytest.skip("Set RAY_PYTEST_USE_GPU=1 to run the two-GPU test")
        torch = pytest.importorskip("torch")
        if torch.cuda.device_count() < 2:
            pytest.skip("Two GPUs are required")
    environment = {
        "UCX_TLS": "tcp,sm,self,cuda_ipc,cuda_copy" if use_cuda else "tcp,sm,self",
        "UCX_SOCKADDR_TLS_PRIORITY": "tcp",
        "UCX_RNDV_THRESH": "8192",
        "AXON_RDT_RECV_TIMEOUT_S": "20",
    }
    for key, value in environment.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def axon_actors(request, monkeypatch):
    use_cuda = request.param
    _configure_axon_environment(use_cuda, monkeypatch)
    ray.init(
        address="local",
        _node_ip_address="127.0.0.1",
        num_cpus=4,
        num_gpus=2 if use_cuda else 0,
        include_dashboard=False,
    )
    actors = [TensorActor.options(num_gpus=int(use_cuda)).remote() for _ in range(2)]
    try:
        yield actors
    finally:
        ray.shutdown()


@pytest.mark.parametrize(
    "backend,axon_actors",
    [("AXON", False), ("AXON_TORCH", False), ("AXON_CUDA", True)],
    indirect=["axon_actors"],
)
def test_builtin_transfer(backend, axon_actors):
    sender, receiver = axon_actors
    # No custom registration, collective group, or explicit initialization.
    ref = sender.produce.options(tensor_transport=backend.lower()).remote(backend)
    assert ray.get(receiver.consume.remote(ref, backend), timeout=45)
    _check_tensors(ray.get(ref, timeout=45)["tensors"], backend)
    [put_ref] = ray.get(sender.put.remote(backend), timeout=30)
    _check_tensors(ray.get(put_ref, timeout=45), backend)


@pytest.mark.parametrize(
    "backend,axon_actors",
    [("AXON", False), ("AXON_TORCH", False), ("AXON_CUDA", True)],
    indirect=["axon_actors"],
)
def test_receiver_can_fetch_while_source_actor_is_busy(backend, axon_actors):
    sender, receiver = axon_actors
    ref = sender.produce.options(tensor_transport=backend).remote(backend)
    # Wait for publication without consuming the object on the receiver.
    ray.get(sender.runtime_state.remote(), timeout=30)
    ready, release = SignalActor.remote(), SignalActor.remote()
    busy = sender.busy.remote(ready, release)
    ray.get(ready.wait.remote(), timeout=30)
    assert ray.get(receiver.consume.remote(ref, backend), timeout=45)
    assert not ray.wait([busy], timeout=0)[0]
    ray.get(release.send.remote(), timeout=15)
    ray.get(busy, timeout=15)


@pytest.mark.parametrize("axon_actors", [True], indirect=True)
def test_cpu_and_cuda_share_loop_until_last_backend_closes(axon_actors):
    sender, receiver = axon_actors
    for _ in range(2):
        for backend in ("AXON_TORCH", "AXON_CUDA"):
            ref = sender.produce.options(tensor_transport=backend).remote(backend)
            assert ray.get(receiver.consume.remote(ref, backend), timeout=45)
            del ref
        gc.collect()
        assert ray.get(receiver.runtime_state.remote()) == {
            "active": ["AXON_CUDA", "AXON_TORCH"],
            "owners": 2,
            "threads": 1,
        }
        ray.get(
            [actor.close_transport.remote("AXON_CUDA") for actor in axon_actors],
            timeout=30,
        )
        assert ray.get(receiver.runtime_state.remote()) == {
            "active": ["AXON_TORCH"],
            "owners": 1,
            "threads": 1,
        }
        ref = sender.produce.options(tensor_transport="axon_torch").remote("AXON_TORCH")
        assert ray.get(receiver.consume.remote(ref, "AXON_TORCH"), timeout=45)
        del ref
        gc.collect()
        ray.get(
            [actor.close_transport.remote("AXON_TORCH") for actor in axon_actors],
            timeout=30,
        )


@pytest.mark.parametrize("use_cuda", [False, True])
def test_ray_shutdown_releases_transports_and_allows_reinitialization(
    use_cuda, monkeypatch
):
    from ray.experimental.rdt import axon_tensor_transport as adapter, util

    _configure_axon_environment(use_cuda, monkeypatch)
    backends = ["AXON", "AXON_TORCH"]
    if use_cuda:
        backends.append("AXON_CUDA")
    previous_managers = {}
    for _ in range(2):
        ray.init(
            address="local",
            _node_ip_address="127.0.0.1",
            num_cpus=2,
            num_gpus=int(use_cuda),
            include_dashboard=False,
        )
        try:
            sender = TensorActor.options(num_gpus=int(use_cuda)).remote()
            received = {}
            for backend in backends:
                ref = sender.produce.options(tensor_transport=backend).remote(backend)
                received[backend] = ray.get(ref, timeout=45)["tensors"]
                _check_tensors(received[backend], backend)
                del ref
            managers = {name: util.transport_managers[name] for name in backends}
            for name, manager in managers.items():
                assert manager is not previous_managers.get(name)
            thread = adapter._runtime_loop_thread
            assert thread.is_alive()
            assert len(adapter._runtime_users) == len(backends)
        finally:
            # No explicit Axon cleanup, including on the actor.
            ray.shutdown()
        assert not ray.is_initialized()
        assert all(name not in util.transport_managers for name in backends)
        assert not adapter._runtime_users
        assert adapter._runtime_loop is None
        assert adapter._runtime_loop_thread is None
        assert not thread.is_alive()
        for name, manager in managers.items():
            assert manager._closed
            assert manager._runtime is None
            # Received tensors can outlive the Ray connection and native runtime.
            _check_tensors(received[name], name)
        previous_managers = managers
        ray.shutdown()


@pytest.mark.parametrize("axon_actors", [False], indirect=True)
def test_graceful_actor_exit_closes_runtime(axon_actors, tmp_path):
    @ray.remote
    class ExitActor:
        def start(self, report_path):
            from pathlib import Path

            from ray.experimental.rdt import axon_tensor_transport as adapter
            from ray.experimental.rdt.util import get_tensor_transport_manager

            manager = get_tensor_transport_manager("AXON")
            manager._ensure_runtime()
            thread = manager._loop_thread
            shutdown = manager.shutdown

            def report_shutdown(timeout=None):
                shutdown(timeout=timeout)
                Path(report_path).write_text(
                    json.dumps(
                        {
                            "closed": manager._closed,
                            "runtime_released": manager._runtime is None,
                            "thread_stopped": not thread.is_alive(),
                            "owners": len(adapter._runtime_users),
                        }
                    )
                )

            manager.shutdown = report_shutdown

        def exit(self):
            ray.actor.exit_actor()

    actor = ExitActor.remote()
    report_path = tmp_path / "axon_shutdown.json"
    try:
        ray.get(actor.start.remote(str(report_path)), timeout=30)
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.exit.remote(), timeout=30)
        wait_for_condition(report_path.exists, timeout=20)
        assert json.loads(report_path.read_text()) == {
            "closed": True,
            "runtime_released": True,
            "thread_stopped": True,
            "owners": 0,
        }
    finally:
        ray.kill(actor)


def test_normal_process_exit_releases_native_runtime():
    code = """
import atexit

def report():
    assert manager._closed
    assert manager._runtime is None
    assert manager._loop_thread is None
    print("Axon runtime released", flush=True)

atexit.register(report)
from ray.experimental.rdt.axon_tensor_transport import AxonNumpyTensorTransport
manager = AxonNumpyTensorTransport()
manager._ensure_runtime()
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=45,
        env={**os.environ, "UCX_TLS": "tcp,sm,self"},
    )
    assert result.returncode == 0, result.stderr
    assert "Axon runtime released" in result.stdout
    assert "Failed to shut down Axon" not in result.stderr
    assert "nanobind: leaked" not in result.stderr


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
