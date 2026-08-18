import re
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Union

# Network modes accepted by runsc.
VALID_NETWORK_MODES = ("none", "host", "sandbox")

# Docker's default Linux capability set. The runtime's own default (whatever
# ``runsc spec`` emits) is far narrower, and container images are
# overwhelmingly built and tested against Docker, so they can break in ways
# that look like image bugs without these: ``apt-get`` forks its download
# methods as the ``_apt`` user (needs CAP_SETUID and CAP_SETGID) and ``tar``
# extracting as root restores the archived owner uid/gid (needs CAP_CHOWN),
# both fatally. Pass ``capabilities=DOCKER_DEFAULT_CAPABILITIES`` to run an
# image the way Docker would.
DOCKER_DEFAULT_CAPABILITIES = [
    "CAP_AUDIT_WRITE",
    "CAP_CHOWN",
    "CAP_DAC_OVERRIDE",
    "CAP_FOWNER",
    "CAP_FSETID",
    "CAP_KILL",
    "CAP_MKNOD",
    "CAP_NET_BIND_SERVICE",
    "CAP_NET_RAW",
    "CAP_SETFCAP",
    "CAP_SETGID",
    "CAP_SETPCAP",
    "CAP_SETUID",
    "CAP_SYS_CHROOT",
]


def parse_memory_bytes(memory: Optional[Union[str, int, float]]) -> Optional[int]:
    """Parse memory specifier string (e.g. '1Gi', '512Mi', '2GB') or number into integer bytes."""
    if memory is None:
        return None
    if isinstance(memory, (int, float)):
        return int(memory)
    if isinstance(memory, str):
        s = memory.strip()
        if not s:
            return None
        if s.isdigit():
            return int(s)

        match = re.match(r"^([0-9.]+)\s*([a-zA-Z]+)?$", s)
        if not match:
            raise ValueError(f"Invalid memory string format: '{memory}'")
        val = float(match.group(1))
        unit = match.group(2)
        if not unit:
            return int(val)
        unit_upper = unit.upper()
        if unit_upper in ("GI", "GIB", "G"):
            multiplier = 1024**3
        elif unit_upper == "GB":
            multiplier = 1000**3
        elif unit_upper in ("MI", "MIB", "M"):
            multiplier = 1024**2
        elif unit_upper == "MB":
            multiplier = 1000**2
        elif unit_upper in ("KI", "KIB", "K"):
            multiplier = 1024
        elif unit_upper == "KB":
            multiplier = 1000
        elif unit_upper == "B":
            multiplier = 1
        else:
            raise ValueError(f"Unknown memory unit in '{memory}'")
        return int(val * multiplier)
    raise TypeError(f"Invalid type for memory: {type(memory)}")


@dataclass
class SandboxConfig:
    """Configuration for a Ray Sandbox instance.

    Attributes:
        image: Container image for the sandbox environment.
        cpu: Number of CPU cores allocated to the sandbox.
        memory: Amount of memory allocated to the sandbox (e.g. "1Gi", "512Mi").
        env: Environment variables to inject into the sandbox.
        workdir: Default working directory inside the sandbox. By default, the
            working directory is the only writable path in the sandbox (unless
            ``readonly=False`` is set). If not provided, the container's WORKDIR is used.
        ttl_seconds: Optional automatic cleanup time-to-live in seconds.
        timeout_seconds: Timeout in seconds for sandbox creation.
        rootless: If True, run gVisor in rootless mode (default: True).
        network: Network mode for runsc ("none", "host", "sandbox") (default: "none").
            With "host", the container shares the host network namespace and the
            host's /etc/resolv.conf is bind-mounted read-only into the container
            (mirroring Docker) so DNS resolution works out of the box.
        capabilities: Optional list of additional Linux capabilities (e.g.
            "CAP_CHOWN") granted to the container process. They are unioned into
            the bounding, effective, inheritable, and permitted sets on top of
            the runtime defaults; the ambient set is deliberately left alone.
            Use :data:`DOCKER_DEFAULT_CAPABILITIES` to match how Docker runs
            images. None (default) keeps the runtime's minimal defaults.
        readonly: If True (default), mount container image rootfs in read-only mode
            such that only ``workdir`` is writable. If False, the entire root filesystem
            is writable. Writes are isolated within a per-sandbox copy-on-write overlay
            filesystem, ensuring multiple sandboxes running the same container image do
            not interfere with each other or modify the base image.
    """

    image: str
    cpu: float = 0.0
    memory: Union[str, int, float] = 0
    env: Dict[str, str] = field(default_factory=dict)
    workdir: Optional[str] = None
    ttl_seconds: Optional[int] = 3600
    timeout_seconds: float = 30.0
    rootless: bool = True
    network: str = "none"
    capabilities: Optional[List[str]] = None
    readonly: bool = True
    _oci_spec_transform_fn: Optional[Callable[[Dict], Optional[Dict]]] = field(
        default=None, repr=False, compare=False
    )
    _ignore_cgroups: bool = field(default=False, repr=False, compare=False)

    def __post_init__(self):
        if not self.image or not isinstance(self.image, str) or not self.image.strip():
            raise ValueError("A valid container image name must be specified.")
        if self.network not in VALID_NETWORK_MODES:
            raise ValueError(
                f"Invalid network mode '{self.network}'. "
                f"Expected one of {VALID_NETWORK_MODES}."
            )


GVisorSandboxConfig = SandboxConfig
