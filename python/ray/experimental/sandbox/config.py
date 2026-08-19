import re
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Union

# Sandbox network modes. "none", "host", and "sandbox" map directly to
# runsc's --network flag; "public" runs with host egress (--network=host)
# but a synthetic, portable /etc/resolv.conf instead of the host's.
VALID_NETWORK_MODES = ("none", "public", "host", "sandbox")

# Default resolvers for network="public" (Google and Cloudflare public DNS).
DEFAULT_PUBLIC_DNS = ("8.8.8.8", "1.1.1.1")

# Docker's default Linux capability set, as documented at
# https://docs.docker.com/engine/containers/run/#runtime-privilege-and-linux-capabilities
# and defined canonically in
# https://github.com/moby/moby/blob/master/oci/caps/defaults.go
# The runtime's own default (whatever ``runsc spec`` emits — see
# https://github.com/google/gvisor/blob/master/runsc/cmd/spec.go) is far
# narrower, and container images are
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
        mount_workdir: Whether to bind-mount a host-backed scratch directory
            at ``workdir``. The bind exists to give a *readonly* rootfs one
            writable path, and it shadows any content the image ships at that
            path — so None (default) derives it from ``readonly``: mounted
            when the rootfs is readonly, otherwise left unmounted so the
            image's own WORKDIR content stays visible (writes then go to the
            per-sandbox overlay). Pass True/False to force either behavior.
        ttl_seconds: Optional automatic cleanup time-to-live in seconds,
            measured wall-clock from creation (not idle time): a sandbox that
            is mid-command when the TTL fires is still deleted. None (default)
            disables it; values <= 0 also mean no TTL.
        timeout_seconds: Timeout in seconds for sandbox creation.
        rootless: If True, run gVisor in rootless mode (default: True).
        network: Network mode (default: "none").
            "none" gives no network access. "public" (recommended for
            internet access) shares the host network namespace for egress but
            uses a synthetic /etc/resolv.conf built from ``dns`` — nothing
            about the host's resolver configuration is inherited, so the
            config stays portable and does not leak host search domains or
            internal resolver addresses. "host" is the power mode: full host
            network identity including the host's own /etc/resolv.conf
            bind-mounted read-only — strictly more permissive than "public"
            (the sandbox can reach internal networks the node can reach).
            "sandbox" uses gVisor's netstack and requires rootless=False.
        dns: Optional list of nameserver IPs written to a generated
            /etc/resolv.conf bind-mounted read-only into the sandbox
            (mirroring ``docker --dns``). Defaults to
            :data:`DEFAULT_PUBLIC_DNS` for network="public"; for
            network="host" it overrides the host's file. Only valid with
            "public" or "host". Locked-down VPCs that block public DNS can
            pass their internal resolver IPs here.
        capabilities: Linux capabilities granted to the container process.
            None (default) keeps the runtime's default set (what ``runsc
            spec`` emits). Otherwise the bounding, effective, and permitted
            sets are set to exactly this list — so ``[]`` runs the sandbox
            with no capabilities at all, and
            :data:`DOCKER_DEFAULT_CAPABILITIES` (a superset of the runtime
            defaults) matches how Docker runs images. The inheritable and
            ambient sets are left untouched, matching modern Docker, which
            stopped setting inheritable capabilities (CVE-2022-24769).
        shell: Shell used to run *string* commands (list commands are run
            argv-style and bypass it). None (default) auto-detects at sandbox
            creation: /bin/bash when the image has it, else /bin/sh. Agent-
            and user-supplied commands overwhelmingly assume bash, and on
            Debian-family images /bin/sh is dash, which fails bashisms
            ([[ ]], pipefail, arrays) with diagnostics that never say "you
            are not in bash".
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
    mount_workdir: Optional[bool] = None
    ttl_seconds: Optional[int] = None
    timeout_seconds: float = 30.0
    rootless: bool = True
    network: str = "none"
    dns: Optional[List[str]] = None
    capabilities: Optional[List[str]] = None
    shell: Optional[str] = None
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
        if self.network == "sandbox" and self.rootless:
            # runsc rejects this at container start, after the image pull and
            # bundle build, with an error naming a flag the user never set.
            raise ValueError(
                "network='sandbox' requires rootless=False; runsc does not "
                "support the sandbox netstack in rootless mode. Use "
                "network='public' or network='host' for a rootless sandbox "
                "with network access."
            )
        if self.dns is not None and self.network not in ("public", "host"):
            raise ValueError(
                "dns is only valid with network='public' or network='host'; "
                f"network={self.network!r} does not mount a resolv.conf."
            )

    @property
    def effective_mount_workdir(self) -> bool:
        """Whether to bind a scratch directory over the container cwd.

        The bind exists to give a readonly rootfs one writable path, so it is
        only needed when the rootfs is readonly. Mounting it on a writable
        rootfs buys nothing and hides whatever the image ships at its WORKDIR.
        """
        if self.mount_workdir is None:
            return self.readonly
        return self.mount_workdir


GVisorSandboxConfig = SandboxConfig
