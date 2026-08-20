"""Pydantic request/response models for the Ray Sandbox HTTP API (v1).

These models are the API contract: the OpenAPI schema FastAPI generates from
them is what clients (e.g. the Harbor ``ray-sandbox`` environment) are written
against. Changing a field here is a contract change; update the snapshot test
in ``tests/test_http_schemas.py`` deliberately when you do.
"""

from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator

# Re-exported so the wire contract documents its default in one place; the
# canonical definition (and rationale) lives in the core sandbox config.
# The API defaults to Docker's set so images behave the way they do under
# Docker; the sets are written exactly, so ``capabilities: []`` runs the
# sandbox with no capabilities at all.
from ray.experimental.sandbox.config import (  # noqa: E402
    DOCKER_DEFAULT_CAPABILITIES,
    VALID_NETWORK_MODES,
)
from ray.util.annotations import PublicAPI

SandboxStatusName = Literal[
    "pending", "pulling", "starting", "running", "error", "terminated"
]
ExecStatusName = Literal["running", "completed", "timeout", "error"]

_MAX_LABELS = 16
_MAX_LABEL_LENGTH = 256


@PublicAPI(stability="alpha")
class ResourceSpec(BaseModel):
    """Resource requests (cluster reservation) and limits (in-sandbox cgroup).

    Requests reserve capacity on the Ray cluster for the actor hosting the
    sandbox; limits cap the sandbox itself via its cgroup. When only a limit
    is given the request defaults to it, so a capped sandbox is also scheduled
    onto capacity that can honor the cap.

    Caveat for CPU: Ray Sandbox derives a cgroup cpu quota from the hosting
    actor's assigned CPUs whenever no explicit cpu limit is set, so a
    request-only CPU spec also ends up capped at the requested value. Memory
    has no such coupling.
    """

    cpu_request: Optional[float] = Field(default=None, gt=0)
    cpu_limit: Optional[float] = Field(default=None, gt=0)
    memory_request_mb: Optional[int] = Field(default=None, gt=0)
    memory_limit_mb: Optional[int] = Field(default=None, gt=0)
    custom: Optional[Dict[str, float]] = Field(
        default=None,
        description=(
            "Custom Ray resources required to schedule the sandbox's hosting "
            'actor, e.g. {"gvisor": 1} to pin sandboxes to runsc-equipped nodes.'
        ),
    )


@PublicAPI(stability="alpha")
class CreateSandboxRequest(BaseModel):
    """Request body for ``POST /api/v1/sandboxes``."""

    image: str = Field(
        min_length=1,
        description=(
            "Container image reference (e.g. 'python:3.12-slim'). Pulled "
            "anonymously from the registry; the image must be public."
        ),
    )
    env: Dict[str, str] = Field(default_factory=dict)
    workdir: Optional[str] = Field(
        default=None,
        description=(
            "Sandbox-level working directory; also the default cwd for execs. "
            "None uses the image's own WORKDIR, like Docker."
        ),
    )
    ttl_seconds: Optional[int] = Field(
        default=3600,
        ge=1,
        description=(
            "Auto-cleanup TTL. The sandbox and its hosting actor are "
            "terminated this many seconds after creation. null disables the "
            "TTL (subject to the server's max_ttl_seconds cap)."
        ),
    )
    network: str = Field(
        default="none",
        description=(
            "runsc network mode; one of " f"{', '.join(VALID_NETWORK_MODES)}."
        ),
    )
    dns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Nameserver IPs for a generated /etc/resolv.conf (mirroring "
            "docker --dns). Defaults to public resolvers for network="
            "'public'; overrides the host file for network='host'."
        ),
    )
    shell: Optional[str] = Field(
        default=None,
        description=(
            "Shell that runs string exec commands (e.g. '/bin/sh' for "
            "images without bash). null uses the sandbox default, /bin/bash."
        ),
    )
    rootless: bool = True
    readonly: bool = True
    resources: Optional[ResourceSpec] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    capabilities: Optional[List[str]] = Field(
        default=None,
        description=(
            "Linux capabilities granted to the sandbox (the sets are "
            "written exactly). null applies the server default (Docker's "
            "14-capability set); [] runs with no capabilities."
        ),
    )
    image_pull_timeout_seconds: float = Field(default=600.0, gt=0)
    start_timeout_seconds: float = Field(default=60.0, gt=0)
    client_token: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=128,
        description=(
            "Idempotency token. Creates with the same token resolve to the "
            "same sandbox, so a client that lost a create response can retry "
            "without leaking a duplicate sandbox."
        ),
    )

    @field_validator("network")
    @classmethod
    def _validate_network(cls, network: str) -> str:
        # Sourced from the core sandbox config so a new Ray mode is accepted
        # here without an API change.
        if network not in VALID_NETWORK_MODES:
            raise ValueError(f"network must be one of {VALID_NETWORK_MODES}")
        return network

    @field_validator("labels")
    @classmethod
    def _validate_labels(cls, labels: Dict[str, str]) -> Dict[str, str]:
        if len(labels) > _MAX_LABELS:
            raise ValueError(f"at most {_MAX_LABELS} labels are allowed")
        for key, value in labels.items():
            if not key or len(key) > _MAX_LABEL_LENGTH:
                raise ValueError(f"label keys must be 1-{_MAX_LABEL_LENGTH} characters")
            if len(value) > _MAX_LABEL_LENGTH:
                raise ValueError(
                    f"label values must be at most {_MAX_LABEL_LENGTH} characters"
                )
        return labels


@PublicAPI(stability="alpha")
class SandboxInfo(BaseModel):
    """Sandbox state as reported by ``GET /api/v1/sandboxes/{id}``."""

    sandbox_id: str
    status: SandboxStatusName
    image: str
    created_at: datetime
    ttl_seconds: Optional[int] = None
    expires_at: Optional[datetime] = None
    network: str
    labels: Dict[str, str] = Field(default_factory=dict)
    error: Optional[str] = Field(
        default=None, description="Failure detail when status is 'error'."
    )


@PublicAPI(stability="alpha")
class SandboxList(BaseModel):
    """Response body for ``GET /api/v1/sandboxes``."""

    sandboxes: List[SandboxInfo]


@PublicAPI(stability="alpha")
class StartExecRequest(BaseModel):
    """Request body for ``POST /api/v1/sandboxes/{id}/execs``.

    A string command runs under the sandbox's shell (``/bin/bash`` unless
    the sandbox or this request configures another); a list is executed
    argv-style without a shell.
    """

    command: Union[str, List[str]]
    cwd: Optional[str] = None
    env: Dict[str, str] = Field(default_factory=dict)
    timeout_seconds: Optional[float] = Field(default=None, gt=0)
    shell: Optional[str] = Field(
        default=None,
        description="Override the sandbox's shell for this command only.",
    )

    @field_validator("command")
    @classmethod
    def _validate_command(cls, command: Union[str, List[str]]) -> Union[str, List[str]]:
        if isinstance(command, str):
            if not command.strip():
                raise ValueError("command must be non-empty")
        elif not command or not all(isinstance(part, str) for part in command):
            raise ValueError("argv command must be a non-empty list of strings")
        return command


@PublicAPI(stability="alpha")
class ExecStarted(BaseModel):
    """Response body for ``POST /api/v1/sandboxes/{id}/execs``."""

    exec_id: str
    status: ExecStatusName


@PublicAPI(stability="alpha")
class ExecInfo(BaseModel):
    """Exec state as reported by ``GET /api/v1/sandboxes/{id}/execs/{exec_id}``."""

    exec_id: str
    status: ExecStatusName
    exit_code: Optional[int] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    duration_seconds: Optional[float] = None
    error: Optional[str] = Field(
        default=None, description="Failure detail when status is 'timeout' or 'error'."
    )


@PublicAPI(stability="alpha")
class SandboxAPISettings(BaseModel):
    """Server-side settings, passed as Serve application builder args."""

    namespace: str = Field(
        default="ray_sandbox_api",
        description="Ray namespace holding the detached per-sandbox actors.",
    )
    token_env_var: str = Field(
        default="RAY_SANDBOX_API_TOKEN",
        description=(
            "Environment variable read at app construction for the bearer "
            "token. Unset/empty disables the app-level check (an Anyscale "
            "service already enforces its own bearer token at the platform "
            "edge)."
        ),
    )
    num_replicas: int = Field(default=1, ge=1)
    max_output_bytes: int = Field(
        default=10 * 1024 * 1024,
        gt=0,
        description="Per-stream cap on retained exec stdout/stderr.",
    )
    max_file_bytes: int = Field(
        default=256 * 1024 * 1024,
        gt=0,
        description="Cap on file upload body size (HTTP 413 above it).",
    )
    max_ttl_seconds: int = Field(default=7 * 24 * 3600, gt=0)
    max_exec_timeout_seconds: float = Field(default=6 * 3600.0, gt=0)
    max_exec_history: int = Field(
        default=256,
        gt=0,
        description="Completed exec records retained per sandbox.",
    )
    default_actor_num_cpus: float = Field(
        default=1.0,
        ge=0,
        description="Actor CPU reservation when the request has no cpu_request.",
    )
    default_capabilities: List[str] = Field(
        default_factory=lambda: list(DOCKER_DEFAULT_CAPABILITIES)
    )
    auto_install_runsc: bool = Field(
        default=False,
        description=(
            "Download gVisor's runsc from the official release bucket onto "
            "any node that lacks it, at first sandbox boot. Lets the service "
            "run on stock images (e.g. an unmodified Anyscale cluster image) "
            "at the cost of a one-time ~40MB download per node. Prefer "
            "baking runsc into the node image for production."
        ),
    )
