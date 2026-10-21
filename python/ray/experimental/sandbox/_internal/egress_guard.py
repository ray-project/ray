"""The egress denylist: destinations a network="public" sandbox never reaches.

Every network="public" sandbox's egress ruleset drops these ahead of its
allowlist, so no entry opens them: ``EGRESS_ALWAYS_DENIED`` (cloud instance
metadata and loopback), the Kubernetes API service, and the operator's
``RAY_SANDBOX_EGRESS_DENY``. The list is static. Ray doesn't track the
cluster's own nodes, whose membership changes as it scales: the default
allowlist, ``internet``, leaves them out while their addresses are internal,
and an operator who allows internal networks lists the nodes' network in
``RAY_SANDBOX_EGRESS_DENY``.
"""

import ipaddress
import os
import socket
from typing import Iterable, List, Tuple

from ray.experimental.sandbox.config import EGRESS_ALWAYS_DENIED, EGRESS_DENY_ENV_VAR


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
