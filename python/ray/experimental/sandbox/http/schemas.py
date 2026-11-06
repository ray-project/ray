"""Pydantic request/response models for the Ray Sandbox HTTP API (v1).

These models are the API contract: the OpenAPI schema FastAPI generates from
them is what clients (e.g. the Harbor ``ray-sandbox`` environment) are written
against. Changing a field here is a contract change; update the snapshot test
in ``tests/test_http_schemas.py`` deliberately when you do.
"""

import os
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator

# Re-exported so the wire contract documents its default in one place; the
# canonical definition (and rationale) lives in the core sandbox config.
# The API defaults to Docker's set so images behave the way they do under
# Docker; the sets are written exactly, so ``capabilities: []`` runs the
# sandbox with no capabilities at all.
from ray.experimental.sandbox.config import (  # noqa: E402
    DOCKER_DEFAULT_CAPABILITIES,
    VALID_NETWORK_MODES,
    normalize_cidr_allowlist,
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

    A request alone sets no cgroup cap: a sandbox with ``cpu_request`` but no
    ``cpu_limit`` reserves that much CPU for its actor and may use idle CPU
    beyond it, and the same holds for memory.
    """

    cpu_request: Optional[float] = Field(default=None, gt=0)
    cpu_limit: Optional[float] = Field(default=None, gt=0)
    memory_request_mb: Optional[int] = Field(default=None, gt=0)
    memory_limit_mb: Optional[int] = Field(default=None, gt=0)
    disk_limit_mb: Optional[int] = Field(
        default=None,
        gt=0,
        description=(
            "Size cap of the sandbox's writable rootfs, in MiB. Writes past it "
            "fail with ENOSPC. Requires readonly=false or a workdir."
        ),
    )
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
        default=None,
        ge=1,
        description=(
            "Auto-cleanup TTL. The sandbox and its hosting actor are "
            "terminated this many seconds after creation. null (the default) "
            "applies the server's max_ttl_seconds; explicit values may not "
            "exceed it."
        ),
    )
    network: str = Field(
        default="none",
        description=(
            "runsc network mode; one of "
            f"{', '.join(VALID_NETWORK_MODES)}. The server accepts only its "
            "allowed_network_modes ('none' and 'public' by default) and "
            "answers 403 for the others."
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
    cidr_allowlist: Optional[List[str]] = Field(
        default=None,
        description=(
            "Egress allowlist for network='public': only the listed "
            "destinations are reachable, plus UDP/TCP port 53 to the dns "
            "resolvers. An entry is an IPv4/IPv6 address or network, or "
            "'internet' for every public address, optionally with a TCP/UDP "
            "port or port range: 10.0.1.5:8000, 10.0.1.0/24:8000-8010, "
            "[2001:db8::1]:443, internet:443. null means the public internet "
            "only, with internal addresses left out as a best effort; [] "
            "allows nothing but DNS. Instance metadata and the Kubernetes API "
            "service stay closed whatever the list says. Ray doesn't track the "
            "cluster's own nodes, so an entry that covers their addresses "
            "reaches them. "
            "Entries are normalized (10.0.1.5/24 becomes 10.0.1.0/24) and "
            "must be IP literals, not host names."
        ),
    )
    shell: Optional[str] = Field(
        default=None,
        description=(
            "Shell that runs string exec commands (e.g. '/bin/sh' for "
            "images without bash). null uses the sandbox default, /bin/bash."
        ),
    )
    rootless: bool = Field(
        default=True,
        description=(
            "Run gVisor rootless. false needs the server's allow_rootful and "
            "otherwise gets 403."
        ),
    )
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

    @model_validator(mode="after")
    def _validate_disk_limit(self) -> "CreateSandboxRequest":
        # The same rule as SandboxConfig's, answered with a 422 up front.
        if (
            self.resources
            and self.resources.disk_limit_mb
            and self.readonly
            and not self.workdir
        ):
            raise ValueError(
                "resources.disk_limit_mb caps the writable rootfs; it requires "
                "readonly=false or a workdir"
            )
        return self

    @model_validator(mode="after")
    def _validate_cidr_allowlist(self) -> "CreateSandboxRequest":
        if self.cidr_allowlist is not None:
            if self.network != "public":
                raise ValueError("cidr_allowlist is only valid with network='public'")
            self.cidr_allowlist = normalize_cidr_allowlist(self.cidr_allowlist)
        return self

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
    cidr_allowlist: Optional[List[str]] = Field(
        default=None,
        description=(
            "Egress allowlist in force for a network='public' sandbox "
            "(['internet'] unless it was created or updated with another); "
            "null for the other modes."
        ),
    )
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
    user: Optional[str] = Field(
        default=None,
        description=(
            "Run as this user: a numeric uid, 'uid:gid', or a name from "
            "the image's /etc/passwd. Default: the image's user."
        ),
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
            "token, which the REST app and the gRPC facade both require on "
            "every call. Their Serve builders refuse to build without it, "
            "because Serve's proxies listen on every node's address; the "
            "REST builder accepts allow_unauthenticated instead. Without "
            "it, the facade's CLI serves only loopback addresses."
        ),
    )
    allow_unauthenticated: bool = Field(
        default=False,
        description=(
            "Let the REST app's Serve builder run without the API token, so "
            "it accepts every request. Only for a deployment where nothing "
            "but an authenticating proxy can reach Serve's proxies: other "
            "pods and processes on the cluster's network reach them "
            "directly. The gRPC facade's builder refuses this setting."
        ),
    )
    allowed_network_modes: List[str] = Field(
        default_factory=lambda: ["none", "public"],
        description=(
            "Network modes REST clients may request. 'host' shares the "
            "node's network, loopback, and every Ray port with the sandbox, "
            "and 'sandbox' needs a rootful runsc, so both are opt-in."
        ),
    )
    allow_rootful: bool = Field(
        default=False,
        description=(
            "Let REST clients request rootless=false, which runs runsc as "
            "the worker's real user rather than in a user namespace."
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
    scheduling_grace_seconds: float = Field(
        default=60.0,
        gt=0,
        description=(
            "How long a request may wait on a sandbox whose hosting actor "
            "has not been scheduled yet before reporting the sandbox as "
            "pending instead of blocking. A cluster that is adding nodes "
            "routinely takes tens of seconds to place an actor; requests "
            "never fail on this budget, they report pending and the client "
            "polls."
        ),
    )
    default_actor_num_cpus: float = Field(
        default=1.0,
        ge=0,
        description="Actor CPU reservation when the request has no cpu_request.",
    )
    default_capabilities: List[str] = Field(
        default_factory=lambda: list(DOCKER_DEFAULT_CAPABILITIES)
    )

    @field_validator("allowed_network_modes")
    @classmethod
    def _validate_allowed_network_modes(cls, modes: List[str]) -> List[str]:
        unknown = [mode for mode in modes if mode not in VALID_NETWORK_MODES]
        if unknown:
            raise ValueError(
                f"allowed_network_modes has unknown modes {unknown}; expected "
                f"a subset of {VALID_NETWORK_MODES}"
            )
        return modes


def _require_token_env(settings: SandboxAPISettings) -> None:
    """Refuse to serve without the API token in this process's environment.

    An app serves every call unchecked without one, and Serve's proxies
    listen on every node's address, which other pods and processes on the
    cluster's network can reach.

    Args:
        settings: The app's settings; ``token_env_var`` names the variable.

    Raises:
        ValueError: If the variable is unset or empty.
    """
    if not os.environ.get(settings.token_env_var):
        raise ValueError(
            f"set {settings.token_env_var} to the API token that clients must "
            "present: Serve's proxies listen on every node's address, which "
            "other pods and processes on the cluster's network can reach"
        )
