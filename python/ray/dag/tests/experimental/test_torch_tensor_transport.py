import ray
import os
import sys
import torch
import logging
import pytest
from typing import Dict, List, Tuple
from ray.dag import InputNode
from ray.air._internal import torch_utils
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa


logger = logging.getLogger(__name__)

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@ray.remote
class TensorDeviceWorker:
    """Worker class for testing tensor transport between different devices."""

    def __init__(self):
        self.default_device = str(torch_utils.get_devices()[0])
        self.default_gpu_ids = ray.get_gpu_ids()

    def get_device_info(self) -> Tuple[str, List[int]]:
        """Get the default device and GPU IDs for this worker."""
        return self.default_device, self.default_gpu_ids

    def send(self, value: int, device: str) -> torch.Tensor:
        """Create a tensor with given value on specified device."""
        return torch.full((100,), value, device=device)

    def echo_tensor_device(self, tensor: torch.Tensor) -> str:
        """Return the device of the input tensor."""
        return str(tensor.device)

    def send_dict(self, name_device_pairs: Dict[str, str]) -> Dict[str, torch.Tensor]:
        """Create a dictionary of tensors on specified devices."""
        tensor_dict = {}
        for name, device in name_device_pairs.items():
            tensor_dict[name] = torch.ones((100,), device=device)
        return tensor_dict

    def echo_dict_device(self, tensor_dict: Dict[str, torch.Tensor]) -> Dict[str, str]:
        """Return the devices of tensors in the input dictionary."""
        return {name: str(tensor.device) for name, tensor in tensor_dict.items()}


@pytest.fixture
def workers() -> Dict[str, ray.actor.ActorHandle]:
    """Fixture to create worker actors with different GPU configurations."""
    return {
        "cpu-only": TensorDeviceWorker.remote(),
        "gpu-1": TensorDeviceWorker.options(num_gpus=1).remote(),
        "gpu-2": TensorDeviceWorker.options(num_gpus=1).remote(),
    }


@pytest.fixture
def multi_gpu_workers() -> Dict[str, ray.actor.ActorHandle]:
    """Fixture to create worker actors for multi-GPU tests."""
    return {
        "gpu-1": TensorDeviceWorker.options(num_gpus=1).remote(),
        "gpu-2": TensorDeviceWorker.options(num_gpus=2).remote(),
    }


@pytest.fixture
def default_device() -> Dict[str, str]:
    """Fixture defining expected default devices for different worker types."""
    return {
        "driver": "cpu",
        "cpu-only": "cpu",
        "gpu-1": "cuda:0",
        "gpu-2": "cuda:0",
    }


@pytest.fixture
def default_device_id() -> Dict[str, List[int]]:
    """Fixture defining expected default devices for different worker types."""
    return {
        "driver": [],
        "cpu-only": [],
        "gpu-1": [0],
        "gpu-2": [1],
    }


class TestTensorTransport:
    """Test suite for PyTorch tensor transport functionality in Ray DAGs.

    Tests cover various scenarios of tensor transport between:
    - Driver and workers
    - Different workers (CPU/GPU)
    - Different seralizers (Ray core vs compiled graph)
    """

    def setup_method(self):
        """Deregister custom serializer before each test to avoid side effects."""
        ray.util.serialization.deregister_serializer(torch.Tensor)

    def teardown_method(self):
        """Cleanup custom serializer after each test."""
        ray.util.serialization.deregister_serializer(torch.Tensor)

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_default_device(
        self, ray_start_regular, workers, default_device, default_device_id
    ):
        """Validate default devices in driver and ray actors."""
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        for worker_name in default_device.keys():
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
                worker_device == default_device[worker_name]
            ), f"expected default device for {worker_name} to be {default_device[worker_name]}, but got {worker_device}"
            assert (
                worker_gpu_ids == default_device_id[worker_name]
            ), f"expected default gpu ids for {worker_name} to be {default_device_id[worker_name]}, but got {worker_gpu_ids}"

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_ray_core_transport_driver_to_worker(self, ray_start_regular, workers):
        """Test tensor transport from driver to worker using Ray core serializer."""
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        for worker in workers.values():
            with InputNode() as inp:
                dag = worker.echo_tensor_device.bind(inp)
            compiled_dag = dag.experimental_compile()
            ref = compiled_dag.execute(torch.tensor([1]))
            assert ray.get(ref) == "cpu"
            compiled_dag.teardown()

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    @pytest.mark.parametrize("device", ["retain", "auto", "cpu", "gpu"])
    def test_compiled_graph_transport_driver_to_worker(
        self, ray_start_regular, workers, default_device, device
    ):
        """Test tensor transport from driver to worker using compiled graph serializer.
        Tests both individual tensors and dictionaries of tensors with mixed CPU/GPU devices.
        """
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        for worker_name, worker in workers.items():
            with InputNode() as inp:
                dag = worker.echo_tensor_device.bind(
                    inp.with_tensor_transport(device=device)
                )
            compiled_dag = dag.experimental_compile()
            if device == "gpu" and worker_name == "cpu-only":
                # skip this case because cpu-only worker doesn't have GPUs
                continue
            ref = compiled_dag.execute(torch.tensor([1]))
            if device == "auto":
                assert ray.get(ref) == default_device[worker_name], worker_name
            elif device == "gpu":
                assert ray.get(ref) == "cuda:0", worker_name
            else:
                assert ray.get(ref) == "cpu", worker_name
            compiled_dag.teardown()

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_ray_core_transport_worker_to_worker_single_tensor(
        self, ray_start_regular, workers
    ):
        """
        This test verifies tensor transport between different types of workers (CPU/GPU)
        using the Ray core default serializer.
        """
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        def create_and_execute_dag(
            src_worker: str,
            dst_worker: str,
            src_device: str,
            expected_dst_device: str,
            expect_error: bool = False,
        ) -> None:
            with InputNode() as inp:
                dag = workers[src_worker].send.bind(inp[0], inp[1])
                dag = workers[dst_worker].echo_tensor_device.bind(dag)
            compiled_dag = dag.experimental_compile()

            try:
                result = ray.get(compiled_dag.execute(1, src_device))
                if expect_error:
                    pytest.fail(f"Expected error but got result: {result}")
                assert result == expected_dst_device
            except RayTaskError as e:
                if not expect_error:
                    raise e
            finally:
                compiled_dag.teardown()

        # Test 1: CPU tensor transport between workers
        create_and_execute_dag("gpu-1", "cpu-only", "cpu", "cpu")
        create_and_execute_dag("cpu-only", "gpu-1", "cpu", "cpu")

        # Test 2: GPU tensor transport between GPU workers
        create_and_execute_dag("gpu-1", "gpu-2", "cuda:0", "cuda:0")
        create_and_execute_dag("gpu-2", "gpu-1", "cuda:0", "cuda:0")

        # Test 3: Error case - GPU tensor to CPU worker
        create_and_execute_dag(
            "gpu-1", "cpu-only", "cuda:0", "cuda:0", expect_error=True
        )

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_ray_core_transport_worker_to_worker_dict_of_tensors(
        self, ray_start_regular, workers
    ):
        """Transport dictionaries of tensors from one worker to another via Ray core default serializer.
        Tests various combinations of CPU/GPU workers and tensor device locations.
        """
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        def create_and_execute_dict_dag(
            src_worker: str,
            dst_worker: str,
            device_dict: Dict[str, str],
            expected_device_dict: Dict[str, str],
            expect_error: bool = False,
        ) -> None:
            with InputNode() as inp:
                dag = workers[src_worker].send_dict.bind(inp)
                dag = workers[dst_worker].echo_dict_device.bind(dag)
            compiled_dag = dag.experimental_compile()

            try:
                result = ray.get(compiled_dag.execute(device_dict))
                if expect_error:
                    pytest.fail(f"Expected error but got result: {result}")
                assert result == expected_device_dict
            except RayTaskError as e:
                if not expect_error:
                    raise e
            finally:
                compiled_dag.teardown()

        mixed_dict = {"tensor1": "cpu", "tensor2": "cuda:0"}
        # Mixed tensor transport between GPU workers
        create_and_execute_dict_dag("gpu-1", "gpu-2", mixed_dict, mixed_dict)

        # Mixed tensor transport to CPU worker
        # Should fail when trying to keep tensors on GPU
        create_and_execute_dict_dag(
            "gpu-1", "cpu-only", mixed_dict, mixed_dict, expect_error=True
        )

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_compiled_graph_transport_worker_to_worker_single_tensor(
        self, ray_start_regular, workers
    ):
        """Transport single tensor from one worker to another via compiled graph serializer."""
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        def create_and_execute_dag(
            src_worker: str,
            dst_worker: str,
            src_device: str,
            expected_dst_device: str,
            transport: str = "auto",
            device: str = "retain",
            expect_error: bool = False,
        ) -> None:
            with InputNode() as inp:
                dag = workers[src_worker].send.bind(inp[0], inp[1])
                dag = dag.with_tensor_transport(transport=transport, device=device)
                dag = workers[dst_worker].echo_tensor_device.bind(dag)
            compiled_dag = dag.experimental_compile()

            try:
                result = ray.get(compiled_dag.execute(1, src_device))
                if expect_error:
                    pytest.fail(f"Expected error but got result: {result}")
                assert result == expected_dst_device
            except RayTaskError as e:
                if not expect_error:
                    print(e)
                    pytest.fail("Failed with an error!")
            finally:
                compiled_dag.teardown()

        # Test 1: CPU tensor transport between workers
        for dst_worker in ["cpu-only", "gpu-2"]:
            for device in ["cpu", "retain"]:
                create_and_execute_dag(
                    "cpu-only", dst_worker, "cpu", "cpu", device=device
                )

        create_and_execute_dag("cpu-only", "gpu-2", "cpu", "cuda:0", device="gpu")
        create_and_execute_dag("cpu-only", "gpu-2", "cpu", "cuda:0", device="auto")

        # # Test 2: GPU tensor transport between GPU workers
        # # Test both auto and nccl transport with retain and auto device placement
        for device in ["retain", "auto"]:
            for transport in ["auto", "nccl"]:
                create_and_execute_dag(
                    "gpu-1", "gpu-2", "cuda:0", "cuda:0", transport, device
                )

        # Test 3: GPU tensor transport to CPU
        # Should succeed with auto/cpu device placement (moves tensor to CPU)
        create_and_execute_dag("gpu-1", "cpu-only", "cuda:0", "cpu", device="auto")
        create_and_execute_dag("gpu-1", "cpu-only", "cuda:0", "cpu", device="cpu")
        create_and_execute_dag("gpu-1", "gpu-2", "cuda:0", "cpu", device="cpu")

        # Should fail with retain device/gpu placement when sending to CPU worker
        create_and_execute_dag(
            "gpu-2", "cpu-only", "cuda:0", "cpu", device="retain", expect_error=True
        )
        create_and_execute_dag(
            "gpu-2", "cpu-only", "cuda:0", "cpu", device="gpu", expect_error=True
        )

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_compiled_graph_transport_worker_to_worker_dict_of_tensors(
        self, ray_start_regular, workers
    ):
        """Transport dictionary of tensors from one worker to another via compiled graph serializer."""
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        assert (
            sum(node["Resources"].get("GPU", 0) for node in ray.nodes()) > 1
        ), "This test requires at least 2 GPUs"

        # Define the input dictionary and expected output dictionaries
        mixed_dict = {"tensor1": "cpu", "tensor2": "cuda:0"}
        all_cpu_dict = {"tensor1": "cpu", "tensor2": "cpu"}
        all_gpu_dict = {"tensor1": "cuda:0", "tensor2": "cuda:0"}

        def create_and_execute_dict_dag(
            src_worker: str,
            dst_worker: str,
            expected_device_dict: Dict[str, str],
            transport: str = "auto",
            device: str = "retain",
            expect_error: bool = False,
        ) -> None:
            with InputNode() as inp:
                dag = workers[src_worker].send_dict.bind(inp)
                dag = dag.with_tensor_transport(transport=transport, device=device)
                dag = workers[dst_worker].echo_dict_device.bind(dag)
            compiled_dag = dag.experimental_compile()

            try:
                result = ray.get(compiled_dag.execute(mixed_dict))
                if expect_error:
                    pytest.fail(f"Expected error but got result: {result}")
                assert result == expected_device_dict
            except RayTaskError as e:
                if not expect_error:
                    raise e
            finally:
                compiled_dag.teardown()

        # Test 1: Transport between GPU workers
        create_and_execute_dict_dag("gpu-1", "gpu-2", mixed_dict, device="retain")

        # With auto: should move everything to GPU
        create_and_execute_dict_dag("gpu-1", "gpu-2", all_gpu_dict, device="auto")

        # With gpu: should move everything to GPU
        create_and_execute_dict_dag("gpu-1", "gpu-2", all_gpu_dict, device="gpu")

        # With cpu: should move everything to CPU
        create_and_execute_dict_dag("gpu-1", "gpu-2", all_cpu_dict, device="cpu")

        # Test 2: Transport to CPU worker
        # With auto/cpu: should move everything to CPU
        create_and_execute_dict_dag("gpu-1", "cpu-only", all_cpu_dict, device="auto")
        create_and_execute_dict_dag("gpu-1", "cpu-only", all_cpu_dict, device="cpu")

        # With retain: should fail since CPU worker can't handle GPU tensors
        create_and_execute_dict_dag(
            "gpu-1", "cpu-only", mixed_dict, device="retain", expect_error=True
        )

        # With gpu: should fail since CPU worker can't handle GPU tensors
        create_and_execute_dict_dag(
            "gpu-1", "cpu-only", all_gpu_dict, device="gpu", expect_error=True
        )

        # Test 3: Test NCCL transport between GPU workers
        for device in ["retain", "auto", "gpu"]:
            expected_dict = mixed_dict if device == "retain" else all_gpu_dict
            create_and_execute_dict_dag(
                "gpu-1", "gpu-2", expected_dict, transport="nccl", device=device
            )

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_transport_multi_gpu_workers(self, ray_start_regular, multi_gpu_workers):
        """Test tensor transport between workers with multiple GPUs."""
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        # Test Ray core transport with invalid device
        with InputNode() as inp:
            dag = multi_gpu_workers["gpu-1"].send.bind(inp[0], inp[1])
            dag = multi_gpu_workers["gpu-2"].echo_tensor_device.bind(dag)
        compiled_dag = dag.experimental_compile()
        with pytest.raises(RayTaskError, match=("CUDA error: invalid device ordinal")):
            ray.get(compiled_dag.execute(1, "cuda:1"))
        compiled_dag.teardown()

        # Test compiled graph with multiple GPUs
        with pytest.raises(
            AssertionError,
            match=(
                "Compiled Graphs currently don't support allocating multiple GPUs "
                "to a single actor"
            ),
        ):
            with InputNode() as inp:
                dag = multi_gpu_workers["gpu-1"].send.bind(inp[0], inp[1])
                dag = dag.with_tensor_transport()
                dag = multi_gpu_workers["gpu-2"].echo_tensor_device.bind(dag)
            compiled_dag = dag.experimental_compile()

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_ray_core_transport_worker_to_driver(self, ray_start_regular, workers):
        """Transport tensors from the worker to the driver via Ray core default serializer.
        Tests both individual tensors and dictionaries of tensors with mixed CPU/GPU devices.
        """
        if not USE_GPU:
            pytest.skip("Test requires GPU")

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
        with InputNode() as inp:
            dag = workers["gpu-1"].send_dict.bind(inp)
        compiled_dag = dag.experimental_compile()
        device_dict = {"cpu_tensor": "cpu", "gpu_tensor": "cuda:0"}
        result = ray.get(compiled_dag.execute(device_dict))
        assert str(result["cpu_tensor"].device) == "cpu"
        assert str(result["gpu_tensor"].device) == "cuda:0"
        compiled_dag.teardown()

    @pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
    def test_compiled_graph_transport_worker_to_driver(
        self, ray_start_regular, workers
    ):
        """Transport tensors from the worker to the driver via compiled graph serializer.
        Tests both individual tensors and dictionaries of tensors with mixed CPU/GPU devices.
        """
        if not USE_GPU:
            pytest.skip("Test requires GPU")

        def create_and_execute_dag(src_worker, src_device, dst_device, device):
            with InputNode() as inp:
                dag = workers[src_worker].send.bind(inp[0], inp[1])
                dag = dag.with_tensor_transport(device=device)
            compiled_dag = dag.experimental_compile()
            tensor = ray.get(compiled_dag.execute(1, src_device))
            assert str(tensor.device) == dst_device
            compiled_dag.teardown()

        # transport cpu tensor
        for device in ["retain", "auto", "cpu"]:
            create_and_execute_dag("gpu-1", "cpu", "cpu", device)

        # transport gpu tensor, assuming driver has access to GPUs
        for device in ["retain", "gpu"]:
            create_and_execute_dag("gpu-1", "cuda:0", "cuda:0", device)

        # transport gpu tensor as a cpu tensor
        for device in ["auto", "cpu"]:
            create_and_execute_dag("gpu-1", "cuda:0", "cpu", device)

        # Test dictionary of tensors, assuming driver has access to GPUs
        def create_and_execute_dict_dag(src_worker, gpu_dst_device, device):
            with InputNode() as inp:
                dag = workers[src_worker].send_dict.bind(inp)
                dag = dag.with_tensor_transport(device=device)
            compiled_dag = dag.experimental_compile()

            device_dict = {"cpu_tensor": "cpu", "gpu_tensor": "cuda:0"}
            result = ray.get(compiled_dag.execute(device_dict))
            # All tensors should be on CPU when received by driver
            assert str(result["cpu_tensor"].device) == "cpu"
            assert str(result["gpu_tensor"].device) == gpu_dst_device
            compiled_dag.teardown()

        create_and_execute_dict_dag("gpu-1", "cpu", "auto")
        create_and_execute_dict_dag("gpu-1", "cpu", "cpu")
        create_and_execute_dict_dag("gpu-1", "cuda:0", "retain")


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
