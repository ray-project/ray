import ray
import os
import sys
import torch
import pytest
from typing import Dict
from ray.dag import InputNode
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa


if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))

# 3: with_tensor_transport(device=...): cpu, gpu (cuda), default
# 3: driver -> worker, worker -> worker, worker -> driver
# 3: src device (cpu tensor, gpu tensor, cpu + gpu)
# 2: dst node (cpu node, gpu node)
# 2: src node (cpu node, gpu node) -> we don't consider this case.


@ray.remote
class Actor:
    def echo_device(self, tensor: torch.Tensor) -> str:
        print("echo device: ", tensor.device)
        return str(tensor.device)

    def echo_dict_device(
        self, dict_of_tensors: Dict[str, torch.Tensor]
    ) -> Dict[str, str]:
        return {k: str(v.device) for k, v in dict_of_tensors.items()}

    def send(self, device: str) -> torch.Tensor:
        print("send device: ", device)
        return torch.ones((100,), device=device)

    def send_dict(self, name_device_pairs: Dict[str, str]) -> Dict[str, torch.Tensor]:
        tensor_dict = {}
        for name, device in name_device_pairs.items():
            tensor_dict[name] = torch.ones((100,), device=device)
        return tensor_dict


class TestDriverToWorkerDeviceCPU:
    # driver -> worker
    # device = CPU
    # 3 (src device) * 2 (dst node)
    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        assert ray.get(ref) == "cpu"

    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        assert ray.get(ref) == "cpu"

    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
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

    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
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
    # driver -> worker
    # device = GPU
    # 3 (src device) * 2 (dst node)
    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular, gpu_device):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device=gpu_device))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))
        with pytest.raises(RayTaskError):
            ray.get(ref)

    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular, gpu_device):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device=gpu_device))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        with pytest.raises(RayTaskError):
            ray.get(ref)

    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular, gpu_device):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device=gpu_device))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular, gpu_device):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device=gpu_device))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular, gpu_device):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_dict_device.bind(
                inp.with_tensor_transport(device=gpu_device)
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(
            {
                "cpu_tensor": torch.tensor([1]),
                "gpu_tensor": torch.tensor([1], device="cuda"),
            }
        )
        with pytest.raises(RayTaskError):
            ray.get(ref)

    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular, gpu_device):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_dict_device.bind(
                inp.with_tensor_transport(device=gpu_device)
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(
            {
                "cpu_tensor": torch.tensor([1]),
                "gpu_tensor": torch.tensor([1], device="cuda"),
            }
        )

        assert ray.get(ref) == {"cpu_tensor": "cuda:0", "gpu_tensor": "cuda:0"}


class TestDriverToWorkerDeviceDefault:
    # driver -> worker
    # device = default
    # 3 (src device) * 2 (dst node)
    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="default"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="default"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        with pytest.raises(RayTaskError):
            ray.get(ref)

    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="default"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1]))
        assert ray.get(ref) == "cpu"

    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.echo_device.bind(inp.with_tensor_transport(device="default"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(torch.tensor([1], device="cuda"))
        assert ray.get(ref) == "cuda:0"

    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
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

        with pytest.raises(RayTaskError):
            ray.get(ref)

    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")
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
    # worker -> worker
    # device = cpu
    # 3 (src device) * 2 (dst node)
    def test_src_cpu_tensor_dst_cpu_node(self, ray_start_regular):
        sender = Actor.remote()
        receiver = Actor.remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        assert ray.get(ref) == "cpu"

    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        sender = Actor.remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        assert ray.get(ref) == "cpu"

    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        assert ray.get(ref) == "cpu"

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(tensor.with_tensor_transport(device="cpu"))
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        assert ray.get(ref) == "cpu"

    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

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

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send_dict.bind(inp)
            dag = receiver.echo_dict_device.bind(
                tensor.with_tensor_transport(device="cpu")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})

        assert ray.get(ref) == {"cpu_tensor": "cpu", "gpu_tensor": "cpu"}


class TestWorkerToWorkerDeviceGPU:
    # worker -> worker
    # device = gpu
    # 3 (src device) * 2 (dst node)
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
        with pytest.raises(RayTaskError):
            ray.get(ref)

    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular, gpu_device):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        sender = Actor.remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(
                tensor.with_tensor_transport(device=gpu_device)
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular, gpu_device):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(
                tensor.with_tensor_transport(device=gpu_device)
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cuda")
        with pytest.raises(RayTaskError):
            ray.get(ref)

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular, gpu_device):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(
                tensor.with_tensor_transport(device=gpu_device)
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cuda")
        assert ray.get(ref) == "cuda:0"

    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular, gpu_device):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options().remote()

        with InputNode() as inp:
            tensor = sender.send_dict.bind(inp)
            dag = receiver.echo_dict_device.bind(
                tensor.with_tensor_transport(device=gpu_device)
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})

        with pytest.raises(RayTaskError):
            ray.get(ref)

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    @pytest.mark.parametrize("gpu_device", ["gpu", "cuda"])
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular, gpu_device):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

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
    # worker -> worker
    # device = default
    # 3 (src device) * 2 (dst node)
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

    def test_src_cpu_tensor_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

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

    def test_src_gpu_tensor_dst_cpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.remote()

        with InputNode() as inp:
            tensor = sender.send.bind(inp)
            dag = receiver.echo_device.bind(
                tensor.with_tensor_transport(device="default")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cuda")
        with pytest.raises(RayTaskError):
            ray.get(ref)

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_gpu_tensor_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

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

    def test_src_mix_tensors_dst_cpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        sender = Actor.options(num_gpus=1).remote()
        receiver = Actor.options().remote()

        with InputNode() as inp:
            tensor = sender.send_dict.bind(inp)
            dag = receiver.echo_dict_device.bind(
                tensor.with_tensor_transport(device="default")
            )
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})

        with pytest.raises(RayTaskError):
            ray.get(ref)

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
    def test_src_mix_tensors_dst_gpu_node(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

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
    # worker -> driver
    # device = cpu
    # driver is a spacial case, which may or may not have access to GPU
    # therefore, we collapse tests to 3 cases
    def test_src_cpu_tensor(self, ray_start_regular):

        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.send.bind(inp).with_tensor_transport(device="cpu")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        tensor = ray.get(ref)
        assert str(tensor.device) == "cpu"

    def test_src_gpu_tensor(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        actor = Actor.options(num_gpus=1).remote()
        with InputNode() as inp:
            dag = actor.send.bind(inp).with_tensor_transport(device="cpu")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cuda")
        tensor = ray.get(ref)
        assert str(tensor.device) == "cpu"

    def test_src_mix_tensors(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        actor = Actor.options(num_gpus=1).remote()

        with InputNode() as inp:
            dag = actor.send_dict.bind(inp).with_tensor_transport(device="cpu")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda"})
        tensor = ray.get(ref)
        assert str(tensor["cpu_tensor"].device) == "cpu"
        assert str(tensor["gpu_tensor"].device) == "cpu"


class TestWorkerToDriverDeviceGPU:
    # worker -> driver
    # device = gpu
    # driver is a spacial case, which may or may not have access to GPU
    # therefore, we collapse tests to 3 cases
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
            with pytest.raises(RayTaskError):
                ray.get(ref)

    def test_src_gpu_tensor(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

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
            with pytest.raises(RayTaskError):
                ray.get(ref)

    def test_src_mix_tensors(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

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
            with pytest.raises(RayTaskError):
                ray.get(ref)


class TestWorkerToDriverDeviceDefault:
    # worker -> driver
    # device = default
    # driver is a spacial case, which may or may not have access to GPU
    # therefore, we collapse tests to 3 cases
    def test_src_cpu_tensor(self, ray_start_regular):
        actor = Actor.remote()

        with InputNode() as inp:
            dag = actor.send.bind(inp).with_tensor_transport(device="default")
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute("cpu")
        tensor = ray.get(ref)
        assert str(tensor.device) == "cpu"

    def test_src_gpu_tensor(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

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
            with pytest.raises(RayTaskError):
                ray.get(ref)

    def test_src_mix_tensors(self, ray_start_regular):
        if not USE_GPU:
            pytest.skip("Test requires GPU")

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
            with pytest.raises(RayTaskError):
                ray.get(ref)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
