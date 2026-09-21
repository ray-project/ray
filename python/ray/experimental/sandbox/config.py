import ipaddress
import re
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Union

# Sandbox network modes. "none", "host", and "sandbox" map directly to runsc
# --network; "public" runs runsc with host networking inside a per-sandbox
# network namespace bridged by slirp4netns (internet egress only; ports and
# loopback are private to the sandbox) plus a generated, host-independent
# resolv.conf.
VALID_NETWORK_MODES = ("none", "public", "host", "sandbox")

# Default resolvers for network="public" (Google and Cloudflare public DNS).
DEFAULT_PUBLIC_DNS = ("8.8.8.8", "1.1.1.1")

# The egress allowlist that permits every destination: what an "open"
# network policy becomes when a sandbox created with an allowlist is widened
# back at runtime.
ALLOW_ALL_CIDRS = ("0.0.0.0/0", "::/0")


def normalize_cidr_allowlist(cidrs: Iterable[str]) -> List[str]:
    """Validate egress allowlist entries and return them in canonical form.

    Every entry must be an IPv4 or IPv6 address or network (``10.0.1.5`` or
    ``10.0.1.0/24``); host bits are cleared, so ``10.0.1.5/24`` becomes
    ``10.0.1.0/24``. The entries end up in an nftables ruleset, so anything
    that is not an address literal, such as a host name, is rejected here
    rather than at sandbox start.

    Args:
        cidrs: The entries to validate.

    Returns:
        The canonical ``str(ipaddress.ip_network(...))`` of each entry, in
        the original order.

    Raises:
        ValueError: If an entry is not an IP address or network.
    """
    if isinstance(cidrs, (str, bytes)):
        raise ValueError(
            f"cidr_allowlist must be a list of CIDR strings, not {cidrs!r}"
        )
    normalized: List[str] = []
    for entry in cidrs:
        try:
            network = ipaddress.ip_network(str(entry).strip(), strict=False)
        except ValueError as err:
            raise ValueError(
                f"cidr_allowlist entry {entry!r} is not an IP address or "
                f"CIDR network: {err}"
            ) from None
        normalized.append(str(network))
    return normalized


# Docker's default capability set (see
# https://docs.docker.com/engine/containers/run/#runtime-privilege-and-linux-capabilities,
# canonical list: https://github.com/moby/moby/blob/master/oci/caps/defaults.go).
# The runtime default (what ``runsc spec`` emits) is far narrower and breaks
# common images: apt-get needs CAP_SETUID/CAP_SETGID, tar-as-root needs
# CAP_CHOWN. Pass this list to run images the way Docker does.
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
        workdir: Working directory for commands (the process cwd). None
            (default) uses the image's WORKDIR, or "/" if the image sets
            none. On a readonly rootfs, an *explicitly* passed workdir is
            also bind-mounted as the sandbox's only writable path
            (host-backed scratch); an inherited image WORKDIR is never
            silently made writable.
        ttl_seconds: Optional time-to-live in seconds, measured wall-clock
            from creation (not idle time). None (default) or <= 0 disables it.
        timeout_seconds: Timeout in seconds for sandbox creation.
        rootless: If True, run gVisor in rootless mode (default: True).
        network: Network mode (default: "none" — no network access).
            "public" (recommended for internet access) gives internet egress
            from a network namespace private to the sandbox, bridged by
            slirp4netns: ports and loopback are per-sandbox, nothing in the
            sandbox is reachable from the host or from other sandboxes, and
            /etc/resolv.conf is generated from ``dns``, inheriting nothing
            from the host resolver. The sandbox can still reach any network
            address the node can reach, including other Ray nodes and
            internal services, so use "none" for untrusted code. Requires
            the ``slirp4netns`` binary on the node. "host" gives full host network
            identity — the host's resolv.conf, internal networks, and a
            port space shared with the worker and every other host-mode
            sandbox.
            "sandbox" uses gVisor's netstack and requires ``rootless=False``.
        dns: Nameserver IPs for a generated /etc/resolv.conf, mounted
            read-only (like ``docker --dns``); useful when public DNS is
            blocked. Defaults to ``DEFAULT_PUBLIC_DNS`` for "public";
            overrides the host file for "host". Only valid with those modes.
        cidr_allowlist: Egress allowlist for network="public". None
            (default) leaves egress unrestricted; a list restricts every
            protocol to destinations inside the listed IPv4/IPv6 networks,
            enforced by an nftables ruleset in the sandbox's private network
            namespace that code inside the sandbox cannot see or change. The
            one exception: UDP and TCP port 53 to the resolvers in ``dns``
            stays open so names still resolve. ``[]`` allows nothing but
            DNS. Entries are normalized (``10.0.1.5/24`` -> ``10.0.1.0/24``)
            and rejected if they are not IP literals. Requires the ``nft``
            binary on the node. Only valid with network="public".
        capabilities: Linux capabilities for the container process. None
            (default) keeps the runtime default (what ``runsc spec`` emits);
            otherwise the bounding/effective/permitted sets are written
            exactly, so ``[]`` means no capabilities. Inheritable and ambient
            stay untouched, matching modern Docker (CVE-2022-24769).
        shell: Shell for *string* commands (list commands bypass it).
            Defaults to "/bin/bash", which string commands overwhelmingly
            assume; set to "/bin/sh" for images without bash.
        readonly: If True (default), the rootfs is read-only and only an
            explicitly passed ``workdir`` is writable — with no workdir,
            nothing on the rootfs is (safe by default; standard tmpfs mounts
            such as /tmp remain writable). If False, the entire rootfs is
            writable through the per-sandbox copy-on-write overlay: image
            content stays visible, sandboxes don't interfere with each
            other, and the base image is never modified.
    """

    image: str
    cpu: float = 0.0
    memory: Union[str, int, float] = 0
    env: Dict[str, str] = field(default_factory=dict)
    workdir: Optional[str] = None
    ttl_seconds: Optional[int] = None
    timeout_seconds: float = 30.0
    rootless: bool = True
    network: str = "none"
    dns: Optional[List[str]] = None
    cidr_allowlist: Optional[List[str]] = None
    capabilities: Optional[List[str]] = None
    shell: str = "/bin/bash"
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
            # runsc only rejects this at container start, after the pull.
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
        if self.cidr_allowlist is not None:
            if self.network != "public":
                raise ValueError(
                    "cidr_allowlist is only valid with network='public', the "
                    "mode that gives the sandbox a private network namespace "
                    f"to filter in; network={self.network!r} has none."
                )
            self.cidr_allowlist = normalize_cidr_allowlist(self.cidr_allowlist)


GVisorSandboxConfig = SandboxConfig
