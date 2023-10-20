import os
import sys
import time
import pytest
from unittest.mock import patch

import ray
from ray._private.accelerators import IntelGPUAcceleratorManager
from ray._private.accelerators import NvidiaGPUAcceleratorManager
from ray.util.accelerators import NVIDIA_TESLA_A100, INTEL_MAX_1550


@patch(
    "ray._private.accelerators.IntelGPUAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=10,
)
def test_task_with_intel_gpu(shutdown_only):
    intel_gpu = INTEL_MAX_1550
    prefix = ray._private.ray_constants.RESOURCE_CONSTRAINT_PREFIX
    resource_name = f"{prefix}{intel_gpu}"
    ray.init(num_cpus=10, num_gpus=10, resources={resource_name: 1})
    assert ray.available_resources()["GPU"] == 10

    def func(num_gpus_per_worker):
        gpu_ids = ray.get_gpu_ids()
        print(f"###################### gpu_ids = {gpu_ids}")
        assert len(gpu_ids) == num_gpus_per_worker
        runtime_gpu_ids = ray.get_runtime_context().get_resource_ids()["GPU"]
        assert len(runtime_gpu_ids) == num_gpus_per_worker
        xpu_selector = os.environ.get("ONEAPI_DEVICE_SELECTOR", None)
        cuda_devices = os.environ.get("CUDA_VISIBLE_DEVICES", None)
        assert cuda_devices is None
        assert xpu_selector is not None
        if num_gpus_per_worker == 0:
            assert xpu_selector == "!level_zero:gpu"
        else:
            gpu_ids_str = ",".join([str(i) for i in gpu_ids])
            assert xpu_selector == f"level_zero:{gpu_ids_str}"
        time.sleep(0.5)
        return num_gpus_per_worker

    objs = []

    # objs.append(ray.remote(num_gpus=0)(lambda: func(0)))
    # objs.append(ray.remote(num_gpus=1)(lambda: func(1)))
    objs.append(ray.remote(num_gpus=2)(lambda: func(2)))
    # objs.append(ray.remote(num_gpus=3)(lambda: func(3)))

    # objs.append(ray.remote(num_gpus=0, accelerator_type=intel_gpu)(lambda: func(0)))
    # objs.append(ray.remote(num_gpus=1, accelerator_type=intel_gpu)(lambda: func(1)))
    # objs.append(ray.remote(num_gpus=2, accelerator_type=intel_gpu)(lambda: func(2)))
    # objs.append(ray.remote(num_gpus=3, accelerator_type=intel_gpu)(lambda: func(3)))

    # objs.append(ray.remote(num_gpus=0, resources={resource_name: 1})(lambda: func(0)))
    # objs.append(ray.remote(num_gpus=1, resources={resource_name: 1})(lambda: func(1)))
    # objs.append(ray.remote(num_gpus=2, resources={resource_name: 1})(lambda: func(2)))
    # objs.append(ray.remote(num_gpus=3, resources={resource_name: 1})(lambda: func(3)))

    ray.get([obj.remote() for obj in objs])


def test_multi_gpu_with_different_vendor(ray_start_cluster):
    cluster = ray_start_cluster
    nvidia_gpu = NVIDIA_TESLA_A100
    intel_gpu = INTEL_MAX_1550
    prefix = ray._private.ray_constants.RESOURCE_CONSTRAINT_PREFIX
    nvidia_resource_name = f"{prefix}{nvidia_gpu}"
    intel_resource_name = f"{prefix}{intel_gpu}"
    cluster.add_node(num_cpus=1, num_gpus=10, resources={nvidia_resource_name: 1})
    cluster.add_node(num_cpus=1, num_gpus=10, resources={intel_resource_name: 1})
    ray.init(address=cluster.address)


@patch(
    "ray._private.accelerators.NvidiaGPUAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=4,
)
def test_auto_detected_more_than_visible_nvidia_gpus(
    mock_get_num_accelerators, monkeypatch, shutdown_only
):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,1,2")
    ray.init()
    mock_get_num_accelerators.called
    print(NvidiaGPUAcceleratorManager.get_current_node_num_accelerators())
    print(ray._private.accelerators.get_accelerator_manager_for_resource("GPU"))
    print(ray.get_gpu_ids())
    assert ray.available_resources()["GPU"] == 3


@patch(
    "ray._private.accelerators.IntelGPUAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=4,
)
def test_auto_detected_more_than_visible_intel_gpus(
    mock_get_num_accelerators, monkeypatch, shutdown_only
):
    monkeypatch.setenv("ONEAPI_DEVICE_SELECTOR", "level_zero:0,1,2")
    ray.init()
    mock_get_num_accelerators.called
    print(IntelGPUAcceleratorManager.get_current_node_num_accelerators())
    print(ray._private.accelerators.get_accelerator_manager_for_resource("GPU"))
    print(f"-------------- ray.get_gpu_ids() = {ray.get_gpu_ids()}")
    assert ray.available_resources()["GPU"] == 3

    @ray.remote(num_gpus=1)
    def task():
        print(ray.get_gpu_ids())
        print(ray._private.accelerators.get_accelerator_manager_for_resource("GPU"))

    ray.get(task.remote())


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
