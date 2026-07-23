from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union


@dataclass
class SandboxConfig:
    """Base configuration for a Ray Sandbox instance.

    Attributes:
        backend: Name of the sandbox backend (e.g. "kubernetes").
        image: Container image for the sandbox environment.
        cpu: Number of CPU cores allocated to the sandbox.
        memory: Amount of memory allocated to the sandbox (e.g. "1Gi", "512Mi").
        env: Environment variables to inject into the sandbox.
        work_dir: Default working directory inside the sandbox.
        ttl_seconds: Optional automatic cleanup time-to-live in seconds.
        labels: Optional key-value metadata labels for tracking.
        timeout_seconds: Timeout in seconds for sandbox creation.
    """

    backend: str = "kubernetes"
    image: str = "python:3.10-slim"
    cpu: float = 1.0
    memory: str = "1Gi"
    env: Dict[str, str] = field(default_factory=dict)
    work_dir: str = "/workspace"
    ttl_seconds: Optional[int] = 3600
    labels: Dict[str, str] = field(default_factory=dict)
    timeout_seconds: float = 30.0


@dataclass
class KubernetesSandboxConfig(SandboxConfig):
    """Kubernetes-specific sandbox configuration.

    Attributes:
        namespace: Kubernetes namespace where the sandbox pod will be created.
        image_pull_policy: Image pull policy for the sandbox container.
        image_pull_secrets: Optional list of secret names for pulling images.
        service_account_name: Optional Kubernetes service account name.
        volumes: Optional custom volume specifications.
        volume_mounts: Optional volume mount specifications.
        security_context: Security context dictionary for container/pod isolation.
        node_selector: Optional node selector label key-value map.
        tolerations: Optional pod tolerations list.
        pod_template: Optional raw dictionary or V1Pod template to base the pod spec on.
        pod_modifier: Optional callback function ``(V1Pod) -> V1Pod`` allowing callers
            to inspect and mutate the Pod specification before API submission.
    """

    backend: str = "kubernetes"
    namespace: str = "default"
    image_pull_policy: str = "IfNotPresent"
    image_pull_secrets: Optional[List[str]] = None
    service_account_name: Optional[str] = None
    volumes: Optional[List[Dict]] = None
    volume_mounts: Optional[List[Dict]] = None
    security_context: Optional[Dict] = None
    node_selector: Optional[Dict[str, str]] = None
    tolerations: Optional[List[Dict]] = None
    pod_template: Optional[Union[Dict, Any]] = None
    pod_modifier: Optional[Callable[[Any], Any]] = None
