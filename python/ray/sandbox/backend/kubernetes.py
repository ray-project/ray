import base64
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Union

from ray.sandbox._internal.file_utils import encode_file_for_write
from ray.sandbox._internal.k8s_client import get_k8s_core_v1_api
from ray.sandbox.backend.base import BaseSandboxBackend, ExecResult, SandboxStatus
from ray.sandbox.config import KubernetesSandboxConfig, SandboxConfig
from ray.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxTimeoutError,
)

logger = logging.getLogger(__name__)

try:
    from kubernetes import client
    from kubernetes.stream import stream

    HAS_K8S = True
except ImportError:
    client = None
    stream = None
    HAS_K8S = False


class KubernetesSandboxBackend(BaseSandboxBackend):
    """Kubernetes sandbox backend managing vanilla Kubernetes Pods."""

    def __init__(self, api_instance=None):
        self._api_instance = api_instance
        self._sandbox_meta: Dict[str, Dict] = {}

    @property
    def api(self):
        if self._api_instance is None:
            self._api_instance = get_k8s_core_v1_api()
        return self._api_instance

    def create_sandbox(self, config: SandboxConfig) -> str:
        """Create a vanilla Kubernetes Pod as a sandbox environment."""
        if not HAS_K8S and self._api_instance is None:
            raise SandboxError(
                "The 'kubernetes' package is required for KubernetesSandboxBackend. "
                "Install via 'pip install kubernetes'."
            )

        if not isinstance(config, KubernetesSandboxConfig):
            k8s_config = KubernetesSandboxConfig(
                backend=config.backend,
                image=config.image,
                cpu=config.cpu,
                memory=config.memory,
                env=config.env,
                work_dir=config.work_dir,
                ttl_seconds=config.ttl_seconds,
                labels=config.labels,
                timeout_seconds=config.timeout_seconds,
            )
        else:
            k8s_config = config

        sandbox_uuid = uuid.uuid4().hex[:12]
        pod_name = f"ray-sb-{sandbox_uuid}"
        sandbox_id = pod_name

        pod_body = self.build_pod_spec(k8s_config, sandbox_id, pod_name)

        try:
            self.api.create_namespaced_pod(
                namespace=k8s_config.namespace,
                body=pod_body,
            )
        except Exception as err:
            raise SandboxCreationError(
                f"Failed to submit Pod creation to Kubernetes API: {err}"
            ) from err

        # Cache metadata for lookup
        self._sandbox_meta[sandbox_id] = {
            "name": pod_name,
            "namespace": k8s_config.namespace,
            "config": k8s_config,
        }

        # Poll until pod is running
        start_time = time.time()
        while time.time() - start_time < k8s_config.timeout_seconds:
            status = self._get_pod_phase(pod_name, k8s_config.namespace)
            if status == SandboxStatus.RUNNING:
                return sandbox_id
            elif status in (SandboxStatus.TERMINATED, SandboxStatus.ERROR):
                raise SandboxCreationError(
                    f"Pod entered unexpected status '{status}' during startup."
                )
            time.sleep(0.1)

        # Cleanup on creation timeout
        self.delete_sandbox(sandbox_id)
        raise SandboxCreationError(
            f"Sandbox pod creation timed out after {k8s_config.timeout_seconds} seconds."
        )

    def build_pod_spec(
        self, k8s_config: KubernetesSandboxConfig, sandbox_id: str, pod_name: str
    ) -> Any:
        """Construct the Kubernetes V1Pod object or spec dictionary.

        This method can be overridden by custom backend subclasses, or customized via
        k8s_config.pod_template and k8s_config.pod_modifier callbacks.
        """
        if k8s_config.pod_template is not None:
            pod_body = k8s_config.pod_template
        elif client is not None:
            env_vars = [
                client.V1EnvVar(name=k, value=str(v)) for k, v in k8s_config.env.items()
            ]
            resources = client.V1ResourceRequirements(
                limits={"cpu": str(k8s_config.cpu), "memory": k8s_config.memory},
                requests={"cpu": str(k8s_config.cpu), "memory": k8s_config.memory},
            )
            container = client.V1Container(
                name="sandbox",
                image=k8s_config.image,
                image_pull_policy=k8s_config.image_pull_policy,
                command=["/bin/sh", "-c", "sleep infinity"],
                working_dir=k8s_config.work_dir,
                env=env_vars,
                resources=resources,
                volume_mounts=k8s_config.volume_mounts,
                security_context=k8s_config.security_context,
            )

            now_ts = int(time.time())
            pod_labels = {
                "app": "ray-sandbox",
                "ray.io/sandbox-id": sandbox_id,
                "ray.io/created-at": str(now_ts),
            }
            if k8s_config.ttl_seconds is not None:
                pod_labels["ray.io/ttl"] = str(k8s_config.ttl_seconds)
            pod_labels.update(k8s_config.labels)

            pod_spec = client.V1PodSpec(
                containers=[container],
                restart_policy="Never",
                service_account_name=k8s_config.service_account_name,
                volumes=k8s_config.volumes,
                node_selector=k8s_config.node_selector,
                tolerations=k8s_config.tolerations,
            )
            if k8s_config.image_pull_secrets:
                pod_spec.image_pull_secrets = [
                    client.V1LocalObjectReference(name=s)
                    for s in k8s_config.image_pull_secrets
                ]

            pod_body = client.V1Pod(
                api_version="v1",
                kind="Pod",
                metadata=client.V1ObjectMeta(
                    name=pod_name,
                    namespace=k8s_config.namespace,
                    labels=pod_labels,
                ),
                spec=pod_spec,
            )
        else:
            # Fallback for unit testing with mocked api_instance
            pod_body = {"name": pod_name, "namespace": k8s_config.namespace}

        # Apply caller's pod_modifier callback if provided
        if k8s_config.pod_modifier is not None:
            modified = k8s_config.pod_modifier(pod_body)
            if modified is not None:
                pod_body = modified

        return pod_body

    def delete_sandbox(self, sandbox_id: str) -> None:
        """Delete Kubernetes Pod for the specified sandbox_id."""
        meta = self._sandbox_meta.get(sandbox_id)
        if meta:
            pod_name = meta["name"]
            namespace = meta["namespace"]
        else:
            pod_name = sandbox_id
            namespace = "default"

        try:
            self.api.delete_namespaced_pod(
                name=pod_name,
                namespace=namespace,
                grace_period_seconds=0,
            )
        except Exception as err:
            logger.debug(
                f"Error deleting pod '{pod_name}' in namespace '{namespace}': {err}"
            )

        self._sandbox_meta.pop(sandbox_id, None)

    def exec_command(
        self,
        sandbox_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecResult:
        """Exec command inside sandbox pod using Kubernetes stream exec."""
        meta = self._get_meta_or_raise(sandbox_id)
        pod_name = meta["name"]
        namespace = meta["namespace"]

        if stream is None and not hasattr(self.api, "connect_get_namespaced_pod_exec"):
            raise SandboxError("kubernetes stream module is required for execution")

        if isinstance(command, list):
            cmd_str = " ".join(command)
        else:
            cmd_str = command

        env_prefix = ""
        if env:
            env_prefix = " ".join(f"{k}='{v}'" for k, v in env.items()) + " "

        work_dir = cwd or meta["config"].work_dir
        wrapped_cmd = f"cd '{work_dir}' && {env_prefix}{cmd_str}"
        exec_req_command = ["/bin/sh", "-c", wrapped_cmd]

        start_time = time.time()
        try:
            if stream is not None:
                resp = stream(
                    self.api.connect_get_namespaced_pod_exec,
                    name=pod_name,
                    namespace=namespace,
                    command=exec_req_command,
                    stderr=True,
                    stdin=False,
                    stdout=True,
                    tty=False,
                    _preload_content=True,
                )
            else:
                resp = self.api.connect_get_namespaced_pod_exec(
                    name=pod_name,
                    namespace=namespace,
                    command=exec_req_command,
                )
            duration = time.time() - start_time
            stdout_str = resp if isinstance(resp, str) else str(resp)
            return ExecResult(
                exit_code=0,
                stdout=stdout_str,
                stderr="",
                duration_seconds=duration,
            )
        except Exception as err:
            duration = time.time() - start_time
            if timeout and duration >= timeout:
                raise SandboxTimeoutError(
                    f"Command execution timed out after {timeout}s: {err}"
                ) from err
            raise SandboxError(f"Exec command failed: {err}") from err

    def write_file(
        self, sandbox_id: str, path: str, content: Union[str, bytes]
    ) -> None:
        """Write content to a file inside the sandbox container."""
        cmd = encode_file_for_write(path, content)
        res = self.exec_command(sandbox_id, cmd)
        if res.exit_code != 0:
            raise SandboxError(f"Failed to write file to '{path}': {res.stderr}")

    def read_file(self, sandbox_id: str, path: str) -> bytes:
        """Read binary content from a file inside the sandbox container."""
        cmd = f"base64 '{path}'"
        res = self.exec_command(sandbox_id, cmd)
        if res.exit_code != 0:
            raise SandboxError(f"Failed to read file from '{path}': {res.stderr}")
        try:
            return base64.b64decode(res.stdout.strip())
        except Exception as err:
            raise SandboxError(
                f"Failed to decode base64 file content for '{path}'"
            ) from err

    def get_status(self, sandbox_id: str) -> SandboxStatus:
        """Query status phase of the sandbox pod."""
        meta = self._get_meta_or_raise(sandbox_id)
        return self._get_pod_phase(meta["name"], meta["namespace"])

    def _get_pod_phase(self, pod_name: str, namespace: str) -> SandboxStatus:
        try:
            pod = self.api.read_namespaced_pod_status(
                name=pod_name, namespace=namespace
            )
            phase = (
                getattr(pod.status, "phase", None) if hasattr(pod, "status") else None
            )
            if phase == "Pending":
                return SandboxStatus.PENDING
            elif phase == "Running":
                return SandboxStatus.RUNNING
            elif phase == "Succeeded":
                return SandboxStatus.TERMINATED
            elif phase == "Failed":
                return SandboxStatus.ERROR
            return SandboxStatus.RUNNING if phase is None else SandboxStatus.PENDING
        except Exception:
            return SandboxStatus.TERMINATED

    def _get_meta_or_raise(self, sandbox_id: str) -> Dict:
        if sandbox_id not in self._sandbox_meta:
            self._sandbox_meta[sandbox_id] = {
                "name": sandbox_id,
                "namespace": "default",
                "config": KubernetesSandboxConfig(),
            }
        return self._sandbox_meta[sandbox_id]
