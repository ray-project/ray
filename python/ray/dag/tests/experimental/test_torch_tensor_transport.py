import ray
import os
import sys
import torch
import logging
import pytest
from ray.dag import InputNode
from ray.air._internal import torch_utils
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa


logger = logging.getLogger(__name__)

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@ray.remote
class MyWorker:
    def __init__(self):
        self.default_device = str(torch_utils.get_devices()[0])
        self.default_gpu_ids = ray.get_gpu_ids()

    def get_device_info(self):
        return self.default_device, self.default_gpu_ids

    def send(self, value: int, device: str):
        return torch.full((100,), value, device=device)

    def echo_tensor_device(self, tensor):
        return str(tensor.device)

    def send_dict(self, name_device_pairs):
        tensor_dict = {}
        for name, device in name_device_pairs.items():
            tensor_dict[name] = torch.ones((100,), device=device)
        return tensor_dict

    def echo_dict_device(self, tensor_dict):
        return {name: str(tensor.device) for name, tensor in tensor_dict.items()}


class TestTensorTransport:
    # All tests inside this file are running in the same process, so we need to
    # manually deregister the custom serializer for `torch.Tensor` before and
    # after each test to avoid side effects.
    def setup_method(self):
        ray.util.serialization.deregister_serializer(torch.Tensor)

    def teardown_method(self):
        ray.util.serialization.deregister_serializer(torch.Tensor)

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_default_device(self, ray_start_regular):
        """Validate default devices in driver and ray actors"""
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        workers = {
            "cpu-only": MyWorker.remote(),
            "gpu-1": MyWorker.options(num_gpus=1).remote(),
            "gpu-2": MyWorker.options(num_gpus=1).remote(),
        }
        default_devices = {
            "driver": ("cpu", []),
            "cpu-only": ("cpu", []),
            "gpu-1": ("cuda:0", [0]),
            "gpu-2": (
                "cuda:0",
                [1],
            ),  # compiled graphs don't handle multiple GPUs in a worker
        }
        for worker_name, (default_device, default_gpu_ids) in default_devices.items():
            if worker_name == "driver":
                worker_device, worker_gpu_ids = (
                    str(torch_utils.get_devices()[0]),
                    ray.get_gpu_ids(),
                )
            else:
                worker_device, worker_gpu_ids = ray.get(
                    workers[worker_name].get_device_info.remote()
                )

            assert (
                worker_device == default_device
            ), f"expected default device for {worker_name} to be {default_device}, but got {worker_device}"
            assert (
                worker_gpu_ids == default_gpu_ids
            ), f"expected default gpu ids for {worker_name} to be {default_gpu_ids}, but got {worker_gpu_ids}"

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_ray_core_transport_driver_to_worker(self, ray_start_regular):
        """Transport tensors from the driver to the worker via Ray core default serializer."""
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        workers = {
            "cpu-only": MyWorker.remote(),
            "gpu-1": MyWorker.options(num_gpus=1).remote(),
            "gpu-2": MyWorker.options(num_gpus=1).remote(),
        }
        for worker in workers.values():
            with InputNode() as inp:
                dag = worker.echo_tensor_device.bind(inp)
            compiled_dag = dag.experimental_compile()
            ref = compiled_dag.execute(torch.tensor([1]))
            assert ray.get(ref) == "cpu"

            compiled_dag.teardown()

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_compiled_graph_transport_driver_to_worker(self, ray_start_regular):
        """Transport tensors from the driver to the worker via compiled graph serializer.
        Tests both individual tensors and dictionaries of tensors with mixed CPU/GPU devices.
        """
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        workers = {
            "cpu-only": MyWorker.remote(),
            "gpu-1": MyWorker.options(num_gpus=1).remote(),
            "gpu-2": MyWorker.options(num_gpus=1).remote(),
        }
        default_devices = {"cpu-only": "cpu", "gpu-1": "cuda:0", "gpu-2": "cuda:0"}

        # Single output node
        for worker_name, worker in workers.items():
            for device_policy in ["auto", "default_device"]:
                with InputNode() as inp:
                    dag = worker.echo_tensor_device.bind(
                        inp.with_tensor_transport(device_policy=device_policy)
                    )
                compiled_dag = dag.experimental_compile()
                ref = compiled_dag.execute(torch.tensor([1]))
                if device_policy == "default_device":
                    assert ray.get(ref) == default_devices[worker_name], worker_name
                else:
                    assert ray.get(ref) == "cpu", worker_name
                compiled_dag.teardown()

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_ray_core_transport_worker_to_worker(self, ray_start_regular):
        """Transport tensors from one worker to another via Ray core default serializer.
        Tests both individual tensors and dictionaries of tensors with mixed CPU/GPU devices.
        """
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        workers = {
            "cpu-only": MyWorker.remote(),
            "gpu-1": MyWorker.options(num_gpus=1).remote(),
            "gpu-2": MyWorker.options(num_gpus=1).remote(),
        }

        # Test transfer of individual tensors
        def execute_dag(src, dst, src_device, raises_exception=False):
            with InputNode() as inp:
                dag = workers[src].send.bind(inp[0], inp[1])
                dag = workers[dst].echo_tensor_device.bind(dag)
            compiled_dag = dag.experimental_compile()
            if not raises_exception:
                dst_device = ray.get(compiled_dag.execute(1, src_device))
                assert (
                    dst_device == src_device
                ), "Ray core serializer failed to retain the same source tensor device!"
            else:
                with pytest.raises(RayTaskError):
                    dst_device = ray.get(compiled_dag.execute(1, src_device))
            compiled_dag.teardown()

        # transfer cpu tensor
        execute_dag("cpu-only", "gpu-1", "cpu")
        execute_dag("gpu-1", "cpu-only", "cpu")

        # transfer gpu tensor
        execute_dag("gpu-1", "gpu-2", "cuda:0")
        execute_dag("gpu-2", "gpu-1", "cuda:0")

        # raise exception when transferring to an unavailable device in destination
        execute_dag("gpu-1", "cpu-only", "cuda:0", True)

        # Test transfer of dictionaries of tensors
        def execute_dict_dag(src, dst, raises_exception=False):
            with InputNode() as inp:
                dag = workers[src].send_dict.bind(inp)
                dag = workers[dst].echo_dict_device.bind(dag)
            compiled_dag = dag.experimental_compile()

            device_dict = {"cpu_tensor": "cpu", "gpu_tensor": "cuda:0"}
            if not raises_exception:
                dst_devices = ray.get(compiled_dag.execute(device_dict))
                assert (
                    dst_devices == device_dict
                ), "Ray core serializer failed to retain the same source tensor dictionary devices!"
            else:
                with pytest.raises(RayTaskError):
                    dst_devices = ray.get(compiled_dag.execute(device_dict))
            compiled_dag.teardown()

        execute_dict_dag("gpu-1", "gpu-2")
        # raise exception when transferring to an unavailable device in destination
        execute_dict_dag("gpu-1", "cpu-only", True)

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_compiled_graph_transport_worker_to_worker(self, ray_start_regular):
        """Transport tensors from one worker to another via compiled graph serializer.
        Tests both individual tensors and dictionaries of tensors with mixed CPU/GPU devices.
        """
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        workers = {
            "cpu-only": MyWorker.remote(),
            "gpu-1": MyWorker.options(num_gpus=1).remote(),
            "gpu-2": MyWorker.options(num_gpus=1).remote(),
        }

        # Test transfer of individual tensors
        def execute_dag(
            src,
            dst,
            src_device,
            dst_default_device,
            transport="auto",
            device_policy="auto",
            raises_exception=False,
        ):
            with InputNode() as inp:
                dag = workers[src].send.bind(inp[0], inp[1])
                dag = dag.with_tensor_transport(transport, device_policy)
                dag = workers[dst].echo_tensor_device.bind(dag)
            compiled_dag = dag.experimental_compile()

            if not raises_exception:
                dst_device = ray.get(compiled_dag.execute(1, src_device))
                assert dst_device == dst_default_device
            else:
                with pytest.raises(RayTaskError):
                    dst_device = ray.get(compiled_dag.execute(1, src_device))
            compiled_dag.teardown()

        # cpu to cpu
        execute_dag("gpu-1", "cpu-only", "cpu", "cpu")

        # cpu to gpu
        execute_dag("cpu-only", "gpu-1", "cpu", "cuda:0", device_policy="default_device")
        execute_dag("cpu-only", "gpu-2", "cpu", "cpu", device_policy="auto")

        # gpu to gpu
        for device_policy in ["default_device", "auto"]:
            for transport in ["auto", "nccl"]:
                execute_dag(
                    "gpu-1", "gpu-2", "cuda:0", "cuda:0", transport, device_policy
                )

        # gpu to cpu
        execute_dag("gpu-1", "cpu-only", "cuda:0", "cpu", device_policy="default_device")
        execute_dag(
            "gpu-2",
            "cpu-only",
            "cuda:0",
            "cpu",
            device_policy="auto",
            raises_exception=True,
        )

        # Test transfer of dictionaries of tensors
        def execute_dict_dag(
            src,
            dst,
            cpu_dst_device,
            gpu_dst_device,
            transport="auto",
            device_policy="auto",
        ):
            with InputNode() as inp:
                dag = workers[src].send_dict.bind(inp)
                dag = dag.with_tensor_transport(transport, device_policy)
                dag = workers[dst].echo_dict_device.bind(dag)
            compiled_dag = dag.experimental_compile()

            dst_devices = ray.get(
                compiled_dag.execute({"cpu_tensor": "cpu", "gpu_tensor": "cuda:0"})
            )
            expected_devices = {
                "cpu_tensor": cpu_dst_device,
                "gpu_tensor": gpu_dst_device,
            }
            assert (
                dst_devices == expected_devices
            ), "compiled graph serializer failed to deserialize tensors on default device!"
            compiled_dag.teardown()

        execute_dict_dag("gpu-1", "cpu-only", "cpu", "cpu", device_policy="default_device")
        for transport in ["auto", "nccl"]:
            execute_dict_dag("gpu-1", "gpu-2", "cuda:0", "cuda:0", transport, "default_device")
            execute_dict_dag("gpu-1", "gpu-2", "cpu", "cuda:0", transport, "auto")

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_transport_multi_gpu_workers(self, ray_start_regular):
        """Transport tensors between workers with multiple GPUs."""
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        workers = {
            "gpu-1": MyWorker.options(num_gpus=1).remote(),
            "gpu-2": MyWorker.options(num_gpus=2).remote(),
        }

        # ray core transport fails due to unavailable device at destination
        with InputNode() as inp:
            dag = workers["gpu-1"].send.bind(inp[0], inp[1])
            dag = workers["gpu-2"].echo_tensor_device.bind(dag)
        compiled_dag = dag.experimental_compile()
        with pytest.raises(RayTaskError, match=("CUDA error: invalid device ordinal")):
            ray.get(compiled_dag.execute(1, "cuda:1"))

        # compiled graphs fails because it doesn't allow multiple devices on an actor
        with pytest.raises(
            AssertionError,
            match=(
                "Compiled Graphs currently don't support allocating multiple GPUs "
                "to a single actor"
            ),
        ):
            with InputNode() as inp:
                dag = workers["gpu-1"].send.bind(inp[0], inp[1])
                dag = dag.with_tensor_transport()
                dag = workers["gpu-2"].echo_tensor_device.bind(dag)
            compiled_dag = dag.experimental_compile()

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_ray_core_transport_worker_to_driver(self, ray_start_regular):
        """Transport tensors from the worker to the driver via Ray core default serializer.
        Tests both individual tensors and dictionaries of tensors with mixed CPU/GPU devices.
        """
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        workers = {
            "cpu-only": MyWorker.remote(),
            "gpu-1": MyWorker.options(num_gpus=1).remote(),
        }

        def execute_dag(src, src_device, dst_device):
            with InputNode() as inp:
                dag = workers[src].send.bind(inp[0], inp[1])
            compiled_dag = dag.experimental_compile()
            tensor = ray.get(compiled_dag.execute(1, src_device))
            assert str(tensor.device) == dst_device
            compiled_dag.teardown()

        # transport cpu tensor
        execute_dag("cpu-only", "cpu", "cpu")
        execute_dag("gpu-1", "cpu", "cpu")
        # transport gpu tensor, assuming driver has access to GPUs
        execute_dag("gpu-1", "cuda:0", "cuda:0")

        # Test dictionary of tensors, assuming driver has access to GPUs
        # assuming driver has access to GPUs
        with InputNode() as inp:
            dag = workers["gpu-1"].send_dict.bind(inp)
        compiled_dag = dag.experimental_compile()
        device_dict = {"cpu_tensor": "cpu", "gpu_tensor": "cuda:0"}
        result = ray.get(compiled_dag.execute(device_dict))
        assert str(result["cpu_tensor"].device) == "cpu"
        assert str(result["gpu_tensor"].device) == "cuda:0"
        compiled_dag.teardown()

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_compiled_graph_transport_worker_to_driver(self, ray_start_regular):
        """Transport tensors from the worker to the driver via compiled graph serializer.
        Tests both individual tensors and dictionaries of tensors with mixed CPU/GPU devices.
        """
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        workers = {
            "cpu-only": MyWorker.remote(),
            "gpu-1": MyWorker.options(num_gpus=1).remote(),
        }

        def execute_dag(src, src_device, dst_device, device_policy):
            with InputNode() as inp:
                dag = workers[src].send.bind(inp[0], inp[1])
                dag = dag.with_tensor_transport(device_policy=device_policy)
            compiled_dag = dag.experimental_compile()
            tensor = ray.get(compiled_dag.execute(1, src_device))
            assert str(tensor.device) == dst_device
            compiled_dag.teardown()

        # transport cpu tensor
        execute_dag("cpu-only", "cpu", "cpu", "auto")
        execute_dag("gpu-1", "cpu", "cpu", "default_device")
        # transport gpu tensor
        execute_dag("gpu-1", "cuda:0", "cpu", "default_device")

        # Test dictionary of tensors
        def execute_dict_dag(src):
            with InputNode() as inp:
                dag = workers[src].send_dict.bind(inp)
                dag = dag.with_tensor_transport(device_policy="default_device")
            compiled_dag = dag.experimental_compile()
            device_dict = {"cpu_tensor": "cpu", "gpu_tensor": "cuda:0"}
            result = ray.get(compiled_dag.execute(device_dict))
            # All tensors should be on CPU when received by driver
            assert str(result["cpu_tensor"].device) == "cpu"
            assert str(result["gpu_tensor"].device) == "cpu"
            compiled_dag.teardown()

        # should be converted to cpu when received by driver
        execute_dict_dag("gpu-1")


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
