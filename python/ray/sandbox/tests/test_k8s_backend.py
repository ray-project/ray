from unittest.mock import MagicMock

from ray.sandbox.backend.base import SandboxStatus
from ray.sandbox.backend.kubernetes import KubernetesSandboxBackend
from ray.sandbox.config import KubernetesSandboxConfig


def test_k8s_backend_create_and_delete():
    mock_api = MagicMock()

    # Mock pod status response for polling check
    mock_pod = MagicMock()
    mock_pod.status.phase = "Running"
    mock_api.read_namespaced_pod_status.return_value = mock_pod

    backend = KubernetesSandboxBackend(api_instance=mock_api)
    config = KubernetesSandboxConfig(
        image="python:3.10-slim",
        namespace="test-ns",
        timeout_seconds=5.0,
    )

    sandbox_id = backend.create_sandbox(config)
    assert sandbox_id.startswith("ray-sb-")

    # Verify K8s API call
    mock_api.create_namespaced_pod.assert_called_once()
    call_args = mock_api.create_namespaced_pod.call_args
    assert call_args.kwargs["namespace"] == "test-ns"

    # Status check
    status = backend.get_status(sandbox_id)
    assert status == SandboxStatus.RUNNING

    # Delete sandbox
    backend.delete_sandbox(sandbox_id)
    mock_api.delete_namespaced_pod.assert_called_once_with(
        name=sandbox_id,
        namespace="test-ns",
        grace_period_seconds=0,
    )


def test_k8s_backend_exec_command():
    mock_api = MagicMock()
    mock_api.connect_get_namespaced_pod_exec.return_value = "Hello from K8s Sandbox\n"

    backend = KubernetesSandboxBackend(api_instance=mock_api)
    res = backend.exec_command("ray-sb-12345", "echo 'Hello from K8s Sandbox'")

    assert res.exit_code == 0
    assert res.stdout == "Hello from K8s Sandbox\n"
    mock_api.connect_get_namespaced_pod_exec.assert_called_once()


def test_k8s_backend_pod_modifier():
    mock_api = MagicMock()
    mock_pod = MagicMock()
    mock_pod.status.phase = "Running"
    mock_api.read_namespaced_pod_status.return_value = mock_pod

    def custom_modifier(pod):
        pod["custom_annotation"] = "custom_value"
        return pod

    backend = KubernetesSandboxBackend(api_instance=mock_api)
    config = KubernetesSandboxConfig(
        pod_modifier=custom_modifier,
        timeout_seconds=5.0,
    )

    sandbox_id = backend.create_sandbox(config)
    assert sandbox_id.startswith("ray-sb-")
    call_args = mock_api.create_namespaced_pod.call_args
    submitted_pod = call_args.kwargs["body"]
    assert submitted_pod["custom_annotation"] == "custom_value"


def test_k8s_backend_pod_template():
    mock_api = MagicMock()
    mock_pod = MagicMock()
    mock_pod.status.phase = "Running"
    mock_api.read_namespaced_pod_status.return_value = mock_pod

    template = {"kind": "Pod", "metadata": {"name": "custom-pod-name"}}
    backend = KubernetesSandboxBackend(api_instance=mock_api)
    config = KubernetesSandboxConfig(
        pod_template=template,
        timeout_seconds=5.0,
    )

    sandbox_id = backend.create_sandbox(config)
    assert sandbox_id.startswith("ray-sb-")
    call_args = mock_api.create_namespaced_pod.call_args
    submitted_pod = call_args.kwargs["body"]
    assert submitted_pod == template
