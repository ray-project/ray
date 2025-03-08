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


class TestDriverToWorkerDeviceCPU:
    """Tests driver to worker tensor transport with CPU device."""

    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_dict_device.bind(inp.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(
            {
                "cpu_tensor": torch.tensor([1]),
                "gpu_tensor": torch.tensor([1], device="cuda"),
            }
        )

        assert ray.get(ref) == {"cpu_tensor": "cpu", "gpu_tensor": "cpu"}

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_dict_device.bind(inp.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(
            {
                "cpu_tensor": torch.tensor([1]),
                "gpu_tensor": torch.tensor([1], device="cuda"),
            }
        )

        assert ray.get(ref) == {"cpu_tensor": "cpu", "gpu_tensor": "cpu"}


class TestDriverToWorkerDeviceGPU:
    """Tests driver to worker tensor transport with GPU device."""

    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cuda"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))

        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cuda"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cuda"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cuda"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_dict_device.bind(inp.with_tensor_transport(device="cuda"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(
            {
                "cpu_tensor": torch.tensor([1]),
                "gpu_tensor": torch.tensor([1], device="cuda"),
            }
        )
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_dict_device.bind(inp.with_tensor_transport(device="cuda"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(
            {
                "cpu_tensor": torch.tensor([1]),
                "gpu_tensor": torch.tensor([1], device="cuda"),
            }
        )

        assert ray.get(ref) == {"cpu_tensor": "cuda:0", "gpu_tensor": "cuda:0"}


class TestDriverToWorkerDeviceDefault:
    """Tests driver to worker tensor transport with default device."""

    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="default"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="default"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="default"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="default"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_dict_device.bind(
                inp.with_tensor_transport(device="default")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(
            {
                "cpu_tensor": torch.tensor([1]),
                "gpu_tensor": torch.tensor([1], device="cuda"),
            }
        )

        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_dict_device.bind(
                inp.with_tensor_transport(device="default")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(
            {
                "cpu_tensor": torch.tensor([1]),
                "gpu_tensor": torch.tensor([1], device="cuda"),
            }
        )

        assert ray.get(ref) == {"cpu_tensor": "cpu", "gpu_tensor": "cuda:0"}


class TestWorkerToWorkerDeviceCPU:
    """Tests worker to worker tensor transport with CPU device."""

    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        sender = Actor.remote()
        receiver = Actor.remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):

        sender = Actor.remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):

        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device="cpu"))
        with pytest.raises(
            ValueError, match="NCCL transport is not supported with CPU target device."
        ):
            dag.experimental_compile()

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options().remote()

        with InputNode() as inp:
            tensor = sender.send_dict.bind(inp)
            dag = receiver.echo_dict_device.bind(
                tensor.with_tensor_transport(device="cpu")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})

        assert ray.get(ref) == {"cpu_tensor": "cpu", "gpu_tensor": "cpu"}

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send_dict.bind(inp)
            dag = receiver.echo_dict_device.bind(
                tensor.with_tensor_transport(device="cpu")
            )
        with pytest.raises(
            ValueError, match="NCCL transport is not supported with CPU target device."
        ):
            dag.experimental_compile()


class TestWorkerToWorkerDeviceGPU:
    """Tests worker to worker tensor transport with GPU device."""

    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular, gpu_device):
        sender = Actor.remote()
        receiver = Actor.remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(
                tensor.with_tensor_transport(device=gpu_device)
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        sender = Actor.remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device="cuda"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device="cuda"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cuda")
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device="cuda"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cuda")
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options().remote()

        with InputNode() as inp:
            tensor = sender.send_dict.bind(inp)
            dag = receiver.echo_dict_device.bind(
                tensor.with_tensor_transport(device="cuda")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})

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

        with InputNode() as inp:
            tensor = sender.send_dict.bind(inp)
            dag = receiver.echo_dict_device.bind(
                tensor.with_tensor_transport(device=gpu_device)
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})

        assert ray.get(ref) == {"cpu_tensor": "cuda:0", "gpu_tensor": "cuda:0"}


class TestWorkerToWorkerDeviceDefault:
    """Tests worker to worker tensor transport with default device."""

    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        sender = Actor.remote()
        receiver = Actor.remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(
                tensor.with_tensor_transport(device="default")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        sender = Actor.remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(
                tensor.with_tensor_transport(device="default")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        assert ray.get(ref) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(
                tensor.with_tensor_transport(device="default")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cuda")
        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(
                tensor.with_tensor_transport(device="default")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cuda")
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options().remote()

        with InputNode() as inp:
            tensor = sender.send_dict.bind(inp)
            dag = receiver.echo_dict_device.bind(
                tensor.with_tensor_transport(device="default")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})

        with pytest.raises(
            RayTaskError, match="RuntimeError: No CUDA GPUs are available"
        ):
            ray.get(ref)

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send_dict.bind(inp)
            dag = receiver.echo_dict_device.bind(
                tensor.with_tensor_transport(device="default")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})

        assert ray.get(ref) == {"cpu_tensor": "cpu", "gpu_tensor": "cuda:0"}


class TestWorkerToDriverDeviceCPU:
    """Tests worker to driver tensor transport with CPU device."""

    def test_src_cpu_tensor(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.send.bind(inp).with_tensor_transport(device="cpu")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        tensor = ray.get(ref)
        assert str(tensor.device) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()
        with InputNode() as inp:
            dag = actor.send.bind(inp).with_tensor_transport(device="cpu")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cuda")
        tensor = ray.get(ref)
        assert str(tensor.device) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_mix_tensors(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.send_dict.bind(inp).with_tensor_transport(device="cpu")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})
        tensor = ray.get(ref)
        assert str(tensor["cpu_tensor"].device) == "cpu"
        assert str(tensor["gpu_tensor"].device) == "cpu"


class TestWorkerToDriverDeviceGPU:
    """Tests worker to driver tensor transport with GPU device."""

    def test_src_cpu_tensor(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.send.bind(inp).with_tensor_transport(device="cuda")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")

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

        with InputNode() as inp:
            dag = actor.send.bind(inp).with_tensor_transport(device="cuda")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cuda")

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

        with InputNode() as inp:
            dag = actor.send_dict.bind(inp).with_tensor_transport(device="cuda")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})

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

        with InputNode() as inp:
            dag = actor.send.bind(inp).with_tensor_transport(device="default")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        tensor = ray.get(ref)
        assert str(tensor.device) == "cpu"

    @pytest.mark.skipif(not USE_GPU, reason="Test requires GPU")
    def test_src_gpu_tensor(self, ray_start_regular):
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.send.bind(inp).with_tensor_transport(device="default")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cuda")

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

        with InputNode() as inp:
            dag = actor.send_dict.bind(inp).with_tensor_transport(device="default")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})

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
