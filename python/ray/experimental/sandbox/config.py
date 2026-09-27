import ipaddress
import math
import re
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

# Sandbox network modes. "none", "host", and "sandbox" map directly to runsc
# --network; "public" runs runsc with host networking inside a per-sandbox
# network namespace bridged by slirp4netns (internet egress only; ports and
# loopback are private to the sandbox) plus a generated, host-independent
# resolv.conf.
VALID_NETWORK_MODES = ("none", "public", "host", "sandbox")

# Default resolvers for network="public" (Google and Cloudflare public DNS).
DEFAULT_PUBLIC_DNS = ("8.8.8.8", "1.1.1.1")

# The egress allowlist that permits every destination outside the denylist
# (EGRESS_ALWAYS_DENIED, the Kubernetes API service, and
# RAY_SANDBOX_EGRESS_DENY): private networks, and any Ray node on them,
# included, unlike the default.
ALLOW_ALL_CIDRS = ("0.0.0.0/0", "::/0")

# The cidr_allowlist entry for the public internet: every destination outside
# INTERNET_EXCLUDED_IPV4, plus IPv6 global unicast (INTERNET_IPV6).
INTERNET = "internet"

# The egress allowlist of a network="public" sandbox created without one: the
# public internet only. Leaving out the internal ranges is a best effort to
# keep sandboxes off the networks where clusters usually run their nodes,
# pods, and services; it covers a Ray node only if the node's address is in
# one of them.
DEFAULT_PUBLIC_ALLOWLIST = (INTERNET,)

# Destinations no allowlist entry opens, ahead of every other rule but DNS:
# "this network", loopback, and link-local, which holds most clouds' instance
# metadata services (169.254.169.254, EKS Pod Identity's 169.254.170.23),
# plus the two that sit outside it: Alibaba Cloud's metadata service, and
# Azure's platform endpoint, which serves VM agent configuration on a public
# address that "internet" would otherwise open. The IPv6 entries are
# link-local, loopback, IPv4-mapped addresses, and AWS's IPv6 metadata
# address; slirp4netns gives sandboxes no IPv6 today, so they only keep
# that true if it ever does.
EGRESS_ALWAYS_DENIED = (
    "0.0.0.0/8",
    "100.100.100.200/32",
    "127.0.0.0/8",
    "168.63.129.16/32",
    "169.254.0.0/16",
    "::1/128",
    "::ffff:0:0/96",
    "fd00:ec2::254/128",
    "fe80::/10",
)

# Environment variable with more destinations to treat like
# EGRESS_ALWAYS_DENIED: comma-separated IP addresses or networks, such as the
# network the Ray nodes use, a Kubernetes API server's endpoint addresses,
# or an external GCS Redis.
EGRESS_DENY_ENV_VAR = "RAY_SANDBOX_EGRESS_DENY"

# IPv4 networks that aren't the public internet: private (RFC 1918), shared
# (RFC 6598), loopback, link-local, the other special-purpose ranges in the
# IANA registry (RFC 6890), multicast, and reserved space. Clusters usually
# run their nodes, pods, and services in them, and slirp4netns's own
# 198.18.0.0/24 is one.
INTERNET_EXCLUDED_IPV4 = (
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.0.0.0/24",
    "192.0.2.0/24",
    "192.88.99.0/24",
    "192.168.0.0/16",
    "198.18.0.0/15",
    "198.51.100.0/24",
    "203.0.113.0/24",
    "224.0.0.0/4",
    "240.0.0.0/4",
)

# IPv6 global unicast. Unique-local, link-local, loopback, and multicast
# addresses are all outside it.
INTERNET_IPV6 = "2000::/3"

_PORTS = re.compile(r"(\d{1,5})(?:-(\d{1,5}))?")


def parse_cidr_allowlist_entry(entry: str) -> Tuple[str, Optional[str]]:
    """Split an egress allowlist entry into its destination and port range.

    An entry is a destination with an optional port or port range, which
    then applies to both TCP and UDP:

    * ``10.0.1.0/24`` or ``10.0.1.5``: an IPv4 or IPv6 network or address.
    * ``10.0.1.5:8000`` or ``10.0.1.0/24:8000-8010``: only those ports.
    * ``[2001:db8::1]:443``: an IPv6 destination with a port, in brackets.
    * ``internet``, or ``internet:443``: every public address, which leaves
      out private and other special-purpose networks (see
      ``INTERNET_EXCLUDED_IPV4``).

    Args:
        entry: The entry to parse.

    Returns:
        The canonical destination (a network in CIDR form, or ``internet``)
        and the canonical port or port range, or None for every port.

    Raises:
        ValueError: If the destination isn't an IP address, network, or
            ``internet``, or the port isn't in 1-65535.
    """
    text = str(entry).strip()
    destination, ports = text, None
    if text.startswith("["):
        destination, close, rest = text[1:].partition("]")
        if not close:
            raise ValueError(f"cidr_allowlist entry {entry!r} has an unclosed '['")
        if rest and not rest.startswith(":"):
            raise ValueError(
                f"cidr_allowlist entry {entry!r} needs ':' and a port after ']'"
            )
        ports = rest[1:] if rest else None
    elif text.count(":") == 1:
        destination, ports = text.split(":")
    if destination.strip().lower() == INTERNET:
        destination = INTERNET
    else:
        try:
            destination = str(ipaddress.ip_network(destination.strip(), strict=False))
        except ValueError as err:
            raise ValueError(
                f"cidr_allowlist entry {entry!r} is not an IP address or "
                f"CIDR network: {err}"
            ) from None
    if ports is not None:
        match = _PORTS.fullmatch(ports.strip())
        low, high = (int(match[1]), int(match[2] or match[1])) if match else (0, 0)
        if not 1 <= low <= high <= 65535:
            raise ValueError(
                f"cidr_allowlist entry {entry!r} needs a port or port range in "
                "1-65535 after the ':'"
            )
        ports = str(low) if low == high else f"{low}-{high}"
    return destination, ports


def normalize_cidr_allowlist(cidrs: Iterable[str]) -> List[str]:
    """Validate egress allowlist entries and return them in canonical form.

    Every entry is an IPv4 or IPv6 address or network (``10.0.1.5`` or
    ``10.0.1.0/24``), or ``internet``, optionally followed by a port or port
    range (see ``parse_cidr_allowlist_entry``). Host bits are cleared, so
    ``10.0.1.5/24`` becomes ``10.0.1.0/24``. The entries end up in an
    nftables ruleset, so anything that is not an address literal, such as a
    host name, is rejected here rather than at sandbox start.

    Args:
        cidrs: The entries to validate.

    Returns:
        Each entry in canonical form (``10.0.1.5/32:8000``,
        ``[2001:db8::1/128]:443``, ``internet``), in the original order.

    Raises:
        ValueError: If an entry is not an IP address or network.
    """
    if isinstance(cidrs, (str, bytes)):
        raise ValueError(
            f"cidr_allowlist must be a list of CIDR strings, not {cidrs!r}"
        )
    normalized: List[str] = []
    for entry in cidrs:
        destination, ports = parse_cidr_allowlist_entry(entry)
        if ports is None:
            normalized.append(destination)
        elif ":" in destination:
            normalized.append(f"[{destination}]:{ports}")
        else:
            normalized.append(f"{destination}:{ports}")
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
            from the host resolver. Egress is filtered by ``cidr_allowlist``,
            the public internet only by default, which leaves out internal
            addresses as a best effort; a denylist keeps instance metadata,
            loopback, and the Kubernetes API service closed whatever the
            allowlist says. For untrusted code that needs the network, set
            an allowlist that names only the services it needs.
            Requires the ``slirp4netns`` and ``nft`` binaries on the node.
            "host" gives full host network identity — the host's
            resolv.conf, internal networks, every Ray port on the node, and a
            port space shared with the worker and every other host-mode
            sandbox — so it suits only trusted code.
            "sandbox" uses gVisor's netstack and requires ``rootless=False``.
        dns: Nameserver IPs for a generated /etc/resolv.conf, mounted
            read-only (like ``docker --dns``); useful when public DNS is
            blocked. Defaults to ``DEFAULT_PUBLIC_DNS`` for "public";
            overrides the host file for "host". Only valid with those modes.
        cidr_allowlist: Egress allowlist for network="public". None
            (default) means ``DEFAULT_PUBLIC_ALLOWLIST``, the public
            internet only. A list restricts every protocol to the listed
            destinations, enforced by an nftables ruleset in the sandbox's
            private network namespace that code inside the sandbox cannot
            see or change. An entry is an IPv4 or IPv6 address or network,
            or ``internet`` for every public address, optionally with a TCP
            and UDP port or port range (``10.0.1.5:8000``,
            ``[2001:db8::1]:443``, ``internet:443``); see
            ``parse_cidr_allowlist_entry``. UDP and TCP port 53 to the
            resolvers in ``dns`` stays open so names still resolve, and
            ``[]`` allows nothing but DNS. No entry opens the denylist:
            ``EGRESS_ALWAYS_DENIED``, the Kubernetes API service, and the
            networks in ``RAY_SANDBOX_EGRESS_DENY``. Ray doesn't track the
            cluster's own nodes, so an entry that covers their addresses,
            such as ``10.0.0.0/8``, reaches them; list their network in
            ``RAY_SANDBOX_EGRESS_DENY`` to keep it closed. Entries are
            normalized (``10.0.1.5/24`` -> ``10.0.1.0/24``), and host names
            are rejected. Only valid with network="public".
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
        disk: Size cap of the writable rootfs overlay (e.g. "10Gi"), in the
            units ``memory`` takes. Writes past it fail with ENOSPC inside
            the sandbox, and the sandbox keeps running. Zero (default) means
            no cap. Requires ``readonly=False`` or a ``workdir``: a readonly
            rootfs stays read-only, so the cap applies to its explicit
            workdir, whose writes then go to a file in the sandbox's
            directory on the node instead of the workdir's host directory.
            In-memory mounts such as /dev/shm count toward the sandbox's
            memory instead.
        gpu_ids: GPU ids or UUIDs, in the format ``ray.get_gpu_ids()`` uses,
            to expose inside the sandbox through CDI. None (default) gives no
            GPU access. Every id must be one Ray assigned the calling actor or
            task, so request GPUs with ``num_gpus``. The backend checks this
            against ``ray.get_gpu_ids()`` when it creates the sandbox. Integer
            ids are stored as strings.
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
    gpu_ids: Optional[List[Union[int, str]]] = None
    _oci_spec_transform_fn: Optional[Callable[[Dict], Optional[Dict]]] = field(
        default=None, repr=False, compare=False
    )
    _ignore_cgroups: bool = field(default=False, repr=False, compare=False)
    disk: Union[str, int, float] = 0

    def __post_init__(self):
        self.cpu = 0.0 if self.cpu is None else float(self.cpu)
        if not math.isfinite(self.cpu) or self.cpu < 0:
            raise ValueError("cpu must be finite and non-negative")
        if 0 < self.cpu < 0.01:
            raise ValueError("cpu must be at least 0.01 for a cgroup CPU quota")
        memory_bytes = parse_memory_bytes(self.memory)
        if memory_bytes is not None and memory_bytes < 0:
            raise ValueError("memory must be non-negative")
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
        disk_bytes = parse_memory_bytes(self.disk) or 0
        if disk_bytes < 0:
            raise ValueError("disk must be non-negative")
        if disk_bytes > 0 and self.readonly and not self.workdir:
            # Only a read-only rootfs without a workdir has no overlay to cap
            # (see ImageManager.create_oci_spec).
            raise ValueError(
                "disk caps the writable rootfs; it requires readonly=False or "
                "a workdir"
            )
        self._validate_gpu_ids()

    def _validate_gpu_ids(self):
        if self.gpu_ids is None:
            return
        if not isinstance(self.gpu_ids, list) or not self.gpu_ids:
            raise ValueError(
                "gpu_ids must be a non-empty list of device ids/UUIDs, "
                f"got {self.gpu_ids!r}."
            )
        if not all(_is_gpu_id(g) for g in self.gpu_ids):
            raise ValueError(
                "gpu_ids must contain only non-empty strings or non-negative "
                f"integers, got {self.gpu_ids!r}."
            )
        self.gpu_ids = [str(g) for g in self.gpu_ids]
        if len(set(self.gpu_ids)) != len(self.gpu_ids):
            raise ValueError(
                f"gpu_ids must not contain duplicates, got {self.gpu_ids!r}."
            )


def _is_gpu_id(gpu_id) -> bool:
    """Whether gpu_id is a non-negative integer index or a non-empty string
    index or UUID, the formats ray.get_gpu_ids() returns."""
    if isinstance(gpu_id, str):
        return bool(gpu_id)
    return isinstance(gpu_id, int) and not isinstance(gpu_id, bool) and gpu_id >= 0


GVisorSandboxConfig = SandboxConfig
