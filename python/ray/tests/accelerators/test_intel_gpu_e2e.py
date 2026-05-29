"""
Manual Intel GPU validation tests, not executed in automated runs.

These tests are basic acceptance tests to validate Intel GPU support in Ray. They
require a suitable Intel GPU environment with dpctl installed. They are intended to
serve as an approved method to verify Intel GPU-based Ray deployments.
"""

import os
import re
from typing import Any, Dict, List

import pytest

import ray

try:
    import dpctl
except ImportError:
    pytest.skip(
        "dpctl is not installed, skipping Intel GPU tests.", allow_module_level=True
    )

DEFAULT_SCALE_OUT_NODES = 2
DEFAULT_SCALE_UP_DEVICES = 2

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))

if not USE_GPU:
    pytest.skip("Skipping, these tests require GPUs.", allow_module_level=True)


@pytest.fixture
def ray_gpu_session():
    """Start a Ray session with caller-provided init kwargs."""

    def _start_session(**init_kwargs):
        if ray.is_initialized():
            ray.shutdown()

        ray.init(**init_kwargs)

    try:
        yield _start_session
    finally:
        if ray.is_initialized():
            ray.shutdown()


def _is_cluster_configured(address: str = "auto") -> bool:
    try:
        ray.init(
            address=address,
        )
        return True
    except (ray.exceptions.RaySystemError, ConnectionError, TimeoutError):
        return False
    finally:
        if ray.is_initialized():
            ray.shutdown()


def _detect_available_gpu_count() -> int:
    """Return the number of GPU devices detected via dpctl."""
    try:
        return dpctl.SyclContext("level_zero:gpu").device_count
    except Exception:
        # If dpctl cannot enumerate devices, assume no additional GPUs.
        return 0


def _require_min_gpus(required: int, context: str) -> None:
    available = _detect_available_gpu_count()
    if available < required:
        pytest.skip(
            f"Skipping {context}: requires {required} GPUs, detected {available} via dpctl."
        )


def _require_min_cluster_nodes(required_nodes: int, context: str) -> None:
    alive_nodes = [node for node in ray.nodes() if node.get("Alive")]
    unique_node_ids = {node.get("NodeID") for node in alive_nodes if node.get("NodeID")}

    if len(unique_node_ids) < required_nodes:
        pytest.skip(
            f"Skipping {context}: requires {required_nodes} alive Ray nodes, detected {len(unique_node_ids)}."
        )


@ray.remote(num_gpus=1)
def gpu_task() -> Dict[str, Any]:
    context = ray.get_runtime_context()
    gpu_ids = context.get_accelerator_ids().get("GPU", [])
    return {
        "gpu_ids": gpu_ids,
        "pid": os.getpid(),
        "oneapi_selector": os.environ.get("ONEAPI_DEVICE_SELECTOR"),
    }


@ray.remote(num_gpus=1)
def cluster_probe_task() -> Dict[str, Any]:
    context = ray.get_runtime_context()
    return {
        "node_id": context.get_node_id(),
        "node_ip": ray.util.get_node_ip_address(),
        "worker_id": context.get_worker_id(),
        "gpu_ids": context.get_accelerator_ids().get("GPU", []),
        "selector": os.environ.get("ONEAPI_DEVICE_SELECTOR"),
    }


def assert_valid_gpu_binding(result: Dict[str, Any], label: str) -> None:
    primary_gpu_id = _validate_gpu_binding_common(result, label)
    assert (
        primary_gpu_id >= 0
    ), f"Expected {label} to bind to a valid GPU, got {result.get('gpu_ids')}"


def _validate_gpu_binding_common(
    result: Dict[str, Any], label: str, selector_key: str = "oneapi_selector"
) -> int:
    """Validate basic GPU binding properties shared by single- and multi-GPU tests."""

    gpu_ids = result.get("gpu_ids")
    assert gpu_ids, f"No GPU IDs assigned for {label}."

    primary_gpu_id = int(gpu_ids[0])

    selector = result.get(selector_key)
    assert selector, f"ONEAPI_DEVICE_SELECTOR not set in environment for {label}."
    selector_lower = selector.lower()
    assert (
        "level_zero:" in selector_lower
    ), f"ONEAPI_DEVICE_SELECTOR should target GPU devices for {label}, got: {selector}."

    selector_gpu_ids = {int(match) for match in re.findall(r"\b\d+\b", selector_lower)}
    assert (
        primary_gpu_id in selector_gpu_ids
    ), f"ONEAPI_DEVICE_SELECTOR does not reference bound GPU id for {label}: {selector}."

    return primary_gpu_id


def assert_valid_multi_gpu_binding(
    results: List[Dict[str, Any]], num_gpus: int, label: str
) -> None:
    """Assert that multiple GPU tasks bind to different GPUs correctly."""
    assert (
        len(results) == num_gpus
    ), f"Expected {num_gpus} results for {label}, got {len(results)}."

    gpu_ids = []
    for i, result in enumerate(results):
        primary_gpu_id = _validate_gpu_binding_common(result, f"{label} instance {i}")
        gpu_ids.append(primary_gpu_id)
    assert (
        len(set(gpu_ids)) == num_gpus
    ), f"Expected {label} to bind to {num_gpus} distinct GPUs, got bindings to GPU IDs: {gpu_ids}."


@pytest.mark.skipif(
    _is_cluster_configured(),
    reason="Environment setup for scale-out, skipping single-node test.",
)
def test_gpu_task_binding(ray_gpu_session) -> None:
    _require_min_gpus(1, "single GPU task binding test")
    ray_gpu_session(num_gpus=1)

    task_result = ray.get(gpu_task.remote())
    assert_valid_gpu_binding(task_result, "GPU task")


@pytest.mark.skipif(
    _is_cluster_configured(),
    reason="Environment setup for scale-out, skipping single-node test.",
)
@pytest.mark.parametrize(
    "num_gpus", [DEFAULT_SCALE_UP_DEVICES]
)  # To be extended to required configurations
def test_multi_gpu_task_binding(ray_gpu_session, num_gpus) -> None:
    """Test that multiple GPU tasks bind to different GPUs correctly."""
    _require_min_gpus(num_gpus, "multi-GPU task binding test")

    ray_gpu_session(num_gpus=num_gpus)

    task_futures = [gpu_task.remote() for _ in range(num_gpus)]
    task_results = ray.get(task_futures)

    assert_valid_multi_gpu_binding(task_results, num_gpus, f"GPU tasks (n={num_gpus})")


@pytest.mark.skipif(
    not _is_cluster_configured(), reason="Environment not setup for scale-out test."
)
@pytest.mark.parametrize(
    "num_nodes", [DEFAULT_SCALE_OUT_NODES]
)  # To be extended to required configurations
def test_scale_out_task_distribution(ray_gpu_session, num_nodes) -> None:
    """Ensure tasks can be scheduled across multiple nodes in the cluster."""
    ray_gpu_session(address="auto")
    _require_min_cluster_nodes(num_nodes, "scale-out task distribution test")

    probe_handles = [
        cluster_probe_task.options(scheduling_strategy="SPREAD").remote()
        for _ in range(num_nodes)
    ]
    probe_results = ray.get(probe_handles)

    node_ids = {
        result.get("node_id") for result in probe_results if result.get("node_id")
    }
    node_ips = {
        result.get("node_ip") for result in probe_results if result.get("node_ip")
    }

    for result in probe_results:
        _validate_gpu_binding_common(result, "scale-out probe task", "selector")

    assert len(node_ids) == num_nodes or len(node_ips) == num_nodes, (
        f"Expected probe tasks to execute on {num_nodes} distinct nodes, "
        f"got node_ids={node_ids} node_ips={node_ips}."
    )

    gpu_capable_results = [result for result in probe_results if result.get("gpu_ids")]
    assert (
        len(gpu_capable_results) == num_nodes
    ), "Not all probe tasks reported GPU accelerator bindings in the cluster."
