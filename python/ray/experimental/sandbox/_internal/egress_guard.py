"""The control-plane guard: destinations a network="public" sandbox never reaches.

Every network="public" sandbox's egress ruleset drops these ahead of its
allowlist. ``always_denied`` is what no entry opens: ``EGRESS_ALWAYS_DENIED``
(instance metadata, loopback), the Kubernetes API service, and the operator's
``RAY_SANDBOX_EGRESS_DENY``. ``cluster_addresses`` is the Ray cluster itself:
this worker's addresses and every Ray node's, where GCS, the raylets, the
dashboard and its agents, the Ray client server, Serve's proxies, and the
sandbox facades listen, on all interfaces and without a token unless the
cluster runs with ``RAY_AUTH_MODE=token``.
"""

import ipaddress
import os
import socket
import threading
import time
from typing import Iterable, List, Optional, Tuple

from ray.experimental.sandbox.config import EGRESS_ALWAYS_DENIED, EGRESS_DENY_ENV_VAR

# How long one ray.nodes() answer is reused. Creating sandboxes in a burst
# would otherwise put a GCS call on every create; a node that joins within
# this window is covered by the default allowlist's private-network
# exclusion until the cache refreshes.
_RAY_ADDRESSES_TTL_SECONDS = 30.0

_ray_addresses_lock = threading.Lock()
_ray_addresses_cache: Optional[Tuple[float, Tuple[str, ...]]] = None


def _host_networks(host: str) -> List[str]:
    """Every address ``host`` names, as single-address networks.

    Args:
        host: An IPv4 or IPv6 address, or a host name to resolve.

    Returns:
        ``a.b.c.d/32`` and ``x::y/128`` networks.

    Raises:
        OSError: If ``host`` is a name that doesn't resolve.
    """
    host = host.strip().strip("[]")
    try:
        addresses = [ipaddress.ip_address(host.split("%", 1)[0])]
    except ValueError:
        addresses = [
            ipaddress.ip_address(info[4][0].split("%", 1)[0])
            for info in socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
        ]
    return [str(ipaddress.ip_network(address)) for address in addresses]


def collapse(networks: Iterable[str]) -> Tuple[List[str], List[str]]:
    """Merge networks into the fewest non-overlapping IPv4 and IPv6 prefixes.

    nftables refuses an interval set whose elements overlap, such as
    127.0.0.0/8 and 127.0.0.1/32, so set elements go through here.

    Args:
        networks: IPv4 and IPv6 networks in any order, duplicates allowed.

    Returns:
        The IPv4 networks and the IPv6 networks, each sorted.
    """
    parsed = [ipaddress.ip_network(n, strict=False) for n in networks]
    v4 = ipaddress.collapse_addresses(n for n in parsed if n.version == 4)
    v6 = ipaddress.collapse_addresses(n for n in parsed if n.version == 6)
    return [str(n) for n in v4], [str(n) for n in v6]


def always_denied() -> List[str]:
    """The destinations no allowlist entry opens.

    Returns:
        ``EGRESS_ALWAYS_DENIED``, the Kubernetes API service address (from
        ``KUBERNETES_SERVICE_HOST``, which every pod gets), and the networks
        in ``RAY_SANDBOX_EGRESS_DENY``.

    Raises:
        ValueError: If ``RAY_SANDBOX_EGRESS_DENY`` holds something other
            than IP addresses and networks.
        OSError: If ``KUBERNETES_SERVICE_HOST`` is a name that doesn't
            resolve.
    """
    networks = list(EGRESS_ALWAYS_DENIED)
    kubernetes = os.environ.get("KUBERNETES_SERVICE_HOST", "").strip()
    if kubernetes:
        networks.extend(_host_networks(kubernetes))
    for entry in os.environ.get(EGRESS_DENY_ENV_VAR, "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        try:
            networks.append(str(ipaddress.ip_network(entry, strict=False)))
        except ValueError:
            raise ValueError(
                f"{EGRESS_DENY_ENV_VAR} entry {entry!r} is not an IP address "
                "or network"
            ) from None
    return networks


def _local_addresses() -> List[str]:
    """This worker's interface addresses, the ones a raylet here listens on."""
    try:
        import psutil
    except ImportError:
        # Ray vendors psutil, so this only happens outside a Ray install.
        return _host_networks(socket.gethostname())
    networks = []
    for addresses in psutil.net_if_addrs().values():
        for address in addresses:
            if address.family in (socket.AF_INET, socket.AF_INET6):
                networks.extend(_host_networks(address.address))
    return networks


def _ray_addresses() -> Tuple[str, ...]:
    """Every alive Ray node's address and the GCS host's, cached briefly.

    The GCS host is listed on its own because a worker may reach GCS by a
    name rather than a node address: on KubeRay it's the head Service, whose
    cluster IP is no node's.

    Returns:
        Single-address networks; empty when this process isn't connected to
        a Ray cluster.
    """
    import ray

    global _ray_addresses_cache
    if not ray.is_initialized():
        return ()
    # Held across the GCS call, so concurrent creates wait for one refresh
    # rather than each making their own.
    with _ray_addresses_lock:
        now = time.monotonic()
        if (
            _ray_addresses_cache is not None
            and now - _ray_addresses_cache[0] < _RAY_ADDRESSES_TTL_SECONDS
        ):
            return _ray_addresses_cache[1]
        hosts = {
            node["NodeManagerAddress"]
            for node in ray.nodes()
            if node.get("Alive") and node.get("NodeManagerAddress")
        }
        gcs_address = ray.get_runtime_context().gcs_address
        if gcs_address:
            hosts.add(gcs_address.rsplit(":", 1)[0])
        addresses = tuple(n for host in sorted(hosts) for n in _host_networks(host))
        _ray_addresses_cache = (now, addresses)
        return addresses


def cluster_addresses() -> List[str]:
    """The Ray cluster's own addresses, closed to all but pinned entries.

    Returns:
        This worker's interface addresses, every alive Ray node's address,
        and the GCS host's, as single-address networks.

    Raises:
        Exception: Whatever the GCS lookup raised. The caller refuses the
            sandbox rather than start it without the cluster's addresses.
    """
    return _local_addresses() + list(_ray_addresses())
