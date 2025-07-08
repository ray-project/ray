import ray
import os
import sys
import torch
import pytest
from typing import Dict
from ray.dag import InputNode
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa
from ray.exceptions import RaySystemError

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@ray.remote
class Actor:
    def echo_device(self, tensor: torch.Tensor) -> str:
        if isinstance(tensor, RaySystemError):
            raise tensor
        return str(tensor.device)

    def echo_dict_device(
        self, dict_of_tensors: Dict[str, torch.Tensor]
    ) -> Dict[str, str]:
        if isinstance(dict_of_tensors, RaySystemError):
            raise dict_of_tensors
        return {k: str(v.device) for k, v in dict_of_tensors.items()}

    def send(self, device: str) -> torch.Tensor:
        return torch.ones((100,), device=device)

    def send_dict(self, name_device_pairs: Dict[str, str]) -> Dict[str, torch.Tensor]:
        tensor_dict = {}
        for name, device in name_device_pairs.items():
            tensor_dict[name] = torch.ones((100,), device=device)
        return tensor_dict


def run_driver_to_worker_dag(actor, device, tensor_input, is_dict=False):
    """Create and execute a DAG with tensor transport for driver to worker tests.

    Args:
        actor: Ray actor to use
        device: Target device ("cpu", "cuda", or "default")
        tensor_input: Input tensor(s) to execute with
        is_dict: Whether to use dict version of the method

    Returns:
        ray.ObjectRef: Result reference
    """
    with InputNode() as inp:
        method = actor.echo_dict_device if is_dict else actor.echo_device
        dag = method.bind(inp.with_tensor_transport(device=device))
    compiled_dag = dag.experimental_compile()
    return compiled_dag.execute(tensor_input)


def run_worker_to_worker_dag(sender, receiver, device, input_device, is_dict=False):
    """Create and execute a DAG with tensor transport for worker to worker tests.

    Args:
        sender: Sender Ray actor
        receiver: Receiver Ray actor
        device: Target device for tensor transport
        input_device: Device string to pass to sender
        is_dict: Whether to use dict version of the methods

    Returns:
        ray.ObjectRef: Result reference or ValueError for compilation errors
    """
    with InputNode() as inp:
        if is_dict:
            tensor = sender.send_dict.bind(inp)
            dag = receiver.echo_dict_device.bind(
                tensor.with_tensor_transport(device=device)
            )
        else:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device=device))
    compiled_dag = dag.experimental_compile()
    return compiled_dag.execute(input_device)


def run_worker_to_driver_dag(actor, device, input_device, is_dict=False):
    """Create and execute a DAG with tensor transport for worker to driver tests.

    Args:
        actor: Ray actor to use
        device: Target device for tensor transport
        input_device: Device string to pass to actor
        is_dict: Whether to use dict version of the method

    Returns:
        ray.ObjectRef: Result reference
    """
    with InputNode() as inp:
        if is_dict:
            dag = actor.send_dict.bind(inp).with_tensor_transport(device=device)
        else:
            dag = actor.send.bind(inp).with_tensor_transport(device=device)
    compiled_dag = dag.experimental_compile()
    return compiled_dag.execute(input_device)


class TestDriverToWorkerDeviceCPU:
    """Tests driver to worker tensor transport with CPU device."""

    def create_and_execute_dag(self, actor, device, tensor_input, is_dict=False):
        """Create a DAG with tensor transport and execute it."""
        with InputNode() as inp:
            method = actor.echo_dict_device if is_dict else actor.echo_device
            dag = method.bind(inp.with_tensor_transport(device=device))
        compiled_dag = dag.experimental_compile()
        return compiled_dag.execute(tensor_input)

    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()
        ref = run_driver_to_worker_dag(actor, "cpu", torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()
        ref = run_driver_to_worker_dag(actor, "cpu", torch.tensor([1], device="cuda"))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_driver_to_worker_dag(actor, "cpu", torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_driver_to_worker_dag(actor, "cpu", torch.tensor([1], device="cuda"))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()
        tensor_dict = {
            "cpu_tensor": torch.tensor([1]),
            "gpu_tensor": torch.tensor([1], device="cuda"),
        }
        ref = run_driver_to_worker_dag(actor, "cpu", tensor_dict, is_dict=True)
        assert ray.get(ref) == {"cpu_tensor": "cpu", "gpu_tensor": "cpu"}

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        tensor_dict = {
            "cpu_tensor": torch.tensor([1]),
            "gpu_tensor": torch.tensor([1], device="cuda"),
        }
        ref = run_driver_to_worker_dag(actor, "cpu", tensor_dict, is_dict=True)
        assert ray.get(ref) == {"cpu_tensor": "cpu", "gpu_tensor": "cpu"}


class TestDriverToWorkerDeviceGPU:
    """Tests driver to worker tensor transport with GPU device."""

    def create_and_execute_dag(self, actor, device, tensor_input, is_dict=False):
        """Create a DAG with tensor transport and execute it."""
        with InputNode() as inp:
            method = actor.echo_dict_device if is_dict else actor.echo_device
            dag = method.bind(inp.with_tensor_transport(device=device))
        compiled_dag = dag.experimental_compile()
        return compiled_dag.execute(tensor_input)

    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()
        ref = run_driver_to_worker_dag(actor, "cuda", torch.tensor([1]))
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()
        ref = run_driver_to_worker_dag(actor, "cuda", torch.tensor([1], device="cuda"))
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_driver_to_worker_dag(actor, "cuda", torch.tensor([1]))
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_driver_to_worker_dag(actor, "cuda", torch.tensor([1], device="cuda"))
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()
        tensor_dict = {
            "cpu_tensor": torch.tensor([1]),
            "gpu_tensor": torch.tensor([1], device="cuda"),
        }
        ref = run_driver_to_worker_dag(actor, "cuda", tensor_dict, is_dict=True)
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        tensor_dict = {
            "cpu_tensor": torch.tensor([1]),
            "gpu_tensor": torch.tensor([1], device="cuda"),
        }
        ref = run_driver_to_worker_dag(actor, "cuda", tensor_dict, is_dict=True)
        assert ray.get(ref) == {"cpu_tensor": "cuda:0", "gpu_tensor": "cuda:0"}


class TestDriverToWorkerDeviceDefault:
    """Tests driver to worker tensor transport with default device."""

    def create_and_execute_dag(self, actor, device, tensor_input, is_dict=False):
        """Create a DAG with tensor transport and execute it."""
        with InputNode() as inp:
            method = actor.echo_dict_device if is_dict else actor.echo_device
            dag = method.bind(inp.with_tensor_transport(device=device))
        compiled_dag = dag.experimental_compile()
        return compiled_dag.execute(tensor_input)

    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()
        ref = run_driver_to_worker_dag(actor, "default", torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()
        ref = run_driver_to_worker_dag(
            actor, "default", torch.tensor([1], device="cuda")
        )
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_driver_to_worker_dag(actor, "default", torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_driver_to_worker_dag(
            actor, "default", torch.tensor([1], device="cuda")
        )
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()
        tensor_dict = {
            "cpu_tensor": torch.tensor([1]),
            "gpu_tensor": torch.tensor([1], device="cuda"),
        }
        ref = run_driver_to_worker_dag(actor, "default", tensor_dict, is_dict=True)
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        tensor_dict = {
            "cpu_tensor": torch.tensor([1]),
            "gpu_tensor": torch.tensor([1], device="cuda"),
        }
        ref = run_driver_to_worker_dag(actor, "default", tensor_dict, is_dict=True)
        assert ray.get(ref) == {"cpu_tensor": "cpu", "gpu_tensor": "cuda:0"}


class TestWorkerToWorkerDeviceCPU:
    """Tests worker to worker tensor transport with CPU device."""

    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        sender = Actor.remote()
        receiver = Actor.remote()
        ref = run_worker_to_worker_dag(sender, receiver, "cpu", "cpu")
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        sender = Actor.remote()
        receiver = Actor.options(num_gpus=1).remote()
        ref = run_worker_to_worker_dag(sender, receiver, "cpu", "cpu")
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.remote()
        ref = run_worker_to_worker_dag(sender, receiver, "cpu", "cpu")
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with pytest.raises(
            ValueError,
            match="accelerator transport is not supported with CPU target device.",
        ):
            run_worker_to_worker_dag(sender, receiver, "cpu", "cpu")

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options().remote()
        ref = run_worker_to_worker_dag(
            sender,
            receiver,
            "cpu",
            {"cpu_tensor": "cpu", "gpu_tensor": "cuda"},
            is_dict=True,
        )
        assert ray.get(ref) == {"cpu_tensor": "cpu", "gpu_tensor": "cpu"}

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with pytest.raises(
            ValueError,
            match="accelerator transport is not supported with CPU target device.",
        ):
            run_worker_to_worker_dag(
                sender,
                receiver,
                "cpu",
                {"cpu_tensor": "cpu", "gpu_tensor": "cuda"},
                is_dict=True,
            )


class TestWorkerToWorkerDeviceGPU:
    """Tests worker to worker tensor transport with GPU device."""

    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular, gpu_device):
        sender = Actor.remote()
        receiver = Actor.remote()
        ref = run_worker_to_worker_dag(sender, receiver, gpu_device, "cpu")
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        sender = Actor.remote()
        receiver = Actor.options(num_gpus=1).remote()
        ref = run_worker_to_worker_dag(sender, receiver, "cuda", "cpu")
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.remote()
        ref = run_worker_to_worker_dag(sender, receiver, "cuda", "cuda")
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with pytest.raises(
            ValueError,
            match="accelerator transport is not supported with CPU target device.",
        ):
            run_worker_to_worker_dag(sender, receiver, "cpu", "cpu")

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options().remote()
        ref = run_worker_to_worker_dag(
            sender,
            receiver,
            "cuda",
            {"cpu_tensor": "cpu", "gpu_tensor": "cuda"},
            is_dict=True,
        )
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular, gpu_device):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        ref = run_worker_to_worker_dag(
            sender,
            receiver,
            gpu_device,
            {"cpu_tensor": "cpu", "gpu_tensor": "cuda"},
            is_dict=True,
        )
        assert ray.get(ref) == {"cpu_tensor": "cuda:0", "gpu_tensor": "cuda:0"}


class TestWorkerToWorkerDeviceDefault:
    """Tests worker to worker tensor transport with default device."""

    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        sender = Actor.remote()
        receiver = Actor.remote()
        ref = run_worker_to_worker_dag(sender, receiver, "default", "cpu")
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        sender = Actor.remote()
        receiver = Actor.options(num_gpus=1).remote()
        ref = run_worker_to_worker_dag(sender, receiver, "default", "cpu")
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.remote()
        ref = run_worker_to_worker_dag(sender, receiver, "default", "cuda")
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with pytest.raises(
            ValueError,
            match="accelerator transport is not supported with CPU target device.",
        ):
            run_worker_to_worker_dag(sender, receiver, "cpu", "cpu")

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options().remote()
        ref = run_worker_to_worker_dag(
            sender,
            receiver,
            "default",
            {"cpu_tensor": "cpu", "gpu_tensor": "cuda"},
            is_dict=True,
        )
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()
        ref = run_worker_to_worker_dag(
            sender,
            receiver,
            "default",
            {"cpu_tensor": "cpu", "gpu_tensor": "cuda"},
            is_dict=True,
        )
        assert ray.get(ref) == {"cpu_tensor": "cpu", "gpu_tensor": "cuda:0"}


class TestWorkerToDriverDeviceCPU:
    """Tests worker to driver tensor transport with CPU device."""

    def test_src_cpu_tensor(self, ray_start_regular):
        actor = Actor.remote()
        ref = run_worker_to_driver_dag(actor, "cpu", "cpu")
        tensor = ray.get(ref)
        assert str(tensor.device) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_worker_to_driver_dag(actor, "cpu", "cuda")
        tensor = ray.get(ref)
        assert str(tensor.device) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_worker_to_driver_dag(
            actor, "cpu", {"cpu_tensor": "cpu", "gpu_tensor": "cuda"}, is_dict=True
        )
        tensor = ray.get(ref)
        assert str(tensor["cpu_tensor"].device) == "cpu"
        assert str(tensor["gpu_tensor"].device) == "cpu"


class TestWorkerToDriverDeviceGPU:
    """Tests worker to driver tensor transport with GPU device."""

    def test_src_cpu_tensor(self, ray_start_regular):
        actor = Actor.remote()
        ref = run_worker_to_driver_dag(actor, "cuda", "cpu")

        # different behavior between a driver node with GPU and without GPU
        if torch.cuda.is_available():
            tensor = ray.get(ref)
            assert str(tensor.device) == "cuda:0"
        else:
            with pytest.raises(
                RayTaskError, match="RuntimeError: No CUDA GPUs are available"
            ):
                ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_worker_to_driver_dag(actor, "cuda", "cuda")

        # different behavior between a driver node with GPU and without GPU
        if torch.cuda.is_available():
            tensor = ray.get(ref)
            assert str(tensor.device) == "cuda:0"
        else:
            with pytest.raises(
                RayTaskError, match="RuntimeError: No CUDA GPUs are available"
            ):
                ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_worker_to_driver_dag(
            actor, "cuda", {"cpu_tensor": "cpu", "gpu_tensor": "cuda"}, is_dict=True
        )

        # different behavior between a driver node with GPU and without GPU
        if torch.cuda.is_available():
            tensor = ray.get(ref)
            assert str(tensor["cpu_tensor"].device) == "cuda:0"
            assert str(tensor["gpu_tensor"].device) == "cuda:0"
        else:
            with pytest.raises(
                RayTaskError, match="RuntimeError: No CUDA GPUs are available"
            ):
                ray.get(ref)


class TestWorkerToDriverDeviceDefault:
    """Tests worker to driver tensor transport with default device."""

    def test_src_cpu_tensor(self, ray_start_regular):
        actor = Actor.remote()
        ref = run_worker_to_driver_dag(actor, "default", "cpu")
        tensor = ray.get(ref)
        assert str(tensor.device) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_worker_to_driver_dag(actor, "default", "cuda")

        # different behavior between a driver node with GPU and without GPU
        if torch.cuda.is_available():
            tensor = ray.get(ref)
            assert str(tensor.device) == "cuda:0"
        else:
            with pytest.raises(
                RayTaskError, match="RuntimeError: No CUDA GPUs are available"
            ):
                ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        ref = run_worker_to_driver_dag(
            actor, "default", {"cpu_tensor": "cpu", "gpu_tensor": "cuda"}, is_dict=True
        )

        # different behavior between a driver node with GPU and without GPU
        if torch.cuda.is_available():
            tensor = ray.get(ref)
            assert str(tensor["cpu_tensor"].device) == "cpu"
            assert str(tensor["gpu_tensor"].device) == "cuda:0"
        else:
            with pytest.raises(
                RayTaskError, match="RuntimeError: No CUDA GPUs are available"
            ):
                ray.get(ref)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
