#
# This is supposed to centralize all internal ray networking construction so
# that we can more easily support IPv6 along with IPv4.
#
import re
import socket
from contextlib import closing


def _get_addrinfo_from_sock_kind(address, kind, family=0):
    """
    Get a list of valid IP addresses compatible with requested filters.

    kind is socket kind such as socket.SOCK_STREAM or socket.SOCK_DGRAM family
    is INET address family.  0 is both IPv4 and IPv6.  socket.AF_INET is IPv4.
    socket.AF_INET6 is IPv6

    Returns a List of tuples with (family, ip) as items.

    Example return value for
    _get_addrinfo_from_sock_kind("localhost", socket.SOCK_STREAM):

        [(socket.AF_INET6, "::1"), (socket.AF_INET, "127.0.0.1")]
    """
    return [
        (x[0], x[-1][0])
        for x in socket.getaddrinfo(address, None, family)
        if x[1] == kind
    ]


def _get_addrinfo_from_sock_kind_ipv4_fallback_ipv6(address, kind):
    """
    Same as _get_sock_kind_from_addrinfo() but favors IPv4 if it is available.

    If IPv4 is not available it will fall back to searching for IPv6.
    """
    inet_addresses = []
    try:
        inet_addresses = _get_addrinfo_from_sock_kind(address, kind, socket.AF_INET)
    except Exception as ignored:  # noqa: F841
        pass
    if len(inet_addresses) > 0:
        return inet_addresses
    else:
        # this call expands to both IPv4 and IPv6 addresses for the given host
        return _get_addrinfo_from_sock_kind(address, kind)


def _get_sock_from_host(address, kind):
    """
    Get a socket.socket for a provided inet address.

    Favor IPv4 but expand to IPv6 if IPv4 not available.
    """
    try:
        # obtain the first valid address for use
        # inet_address is a tuple with (socket.AF_INET or socket.AF_INET6, ip_address)
        inet_address = _get_addrinfo_from_sock_kind_ipv4_fallback_ipv6(address, kind)[0]
    except socket.gaierror as e:
        # This is an undesirable workaround for some existing tests.
        raise ValueError(f"Address invalid or not reachable: {address}\n\n" + repr(e))
    return socket.socket(inet_address[0], kind)


def _get_sock_stream_from_host(address):
    """
    Returns a socket.socket which can either be IPv4 or IPv6 with
    socket.SOCK_STREAM.

    Favor IPv4 but expand to IPv6 if IPv4 not available.
    """
    return _get_sock_from_host(address, socket.SOCK_STREAM)


def _get_sock_dgram_from_host(address):
    """
    Returns a socket.socket which can either be IPv4 or IPv6 with
    socket.SOCK_DGRAM.

    Favor IPv4 but expand to IPv6 if IPv4 not available.
    """
    return _get_sock_from_host(address, socket.SOCK_DGRAM)


def _get_private_ip_addresses():
    """
    Connects to Google DNS over IPv4, IPv6, or both to discover private IP
    addresses.
    """
    private_ip_addresses = []
    for public_host in ["2001:4860:4860::8888", "8.8.8.8"]:
        if ":" in public_host and not socket.has_ipv6:
            continue
        try:
            with closing(_get_sock_dgram_from_host(public_host)) as s:
                # IPv4 may not be available so skip
                if ":" not in public_host and not s.family == socket.AF_INET:
                    continue
                s.connect((public_host, 80))
                # get the local iface IP used to connect against the public_host
                private_ip_addresses.append(s.getsockname()[0])
        except Exception as ignored:  # noqa: F841
            pass
    return private_ip_addresses


def _get_socket_dualstack_fallback_single_stack_laddr(kind=socket.SOCK_STREAM):
    """
    Similar to socket.socket() except that if networking is dualstack it will
    listen on all address family interfaces.

    If single-stack, then the loopback interface is queried for which stack is
    allowed (IPv4 or IPv6).
    """
    if socket.has_dualstack_ipv6():
        sock = socket.socket(socket.AF_INET6, kind)
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        # set up to listen on all IPv4 and IPv6 interfaces with a single socket
        return sock
    else:
        # fall back to supporting the host single network stack.
        # (IPv4 or IPv6 only)
        return _get_sock_from_host("localhost", kind)


def _parse_ip_port(address):
    """
    Parses a str of ip:port and returns a List with (ip, port).

    This uses str.rsplit(":", 1) to support DNS, IPv6, and IPv4 parsing.
    """
    # dashboard passes bytes instead of str so coerce
    if isinstance(address, bytes):
        address = address.decode("ascii")

    if not isinstance(address, str):
        raise ValueError(
            "address type not str or bytes:\n\n"
            + f"    type: {repr(type(address))}\n"
            + f"   value: {repr(address)}"
        )

    # If no colon is in the address then assume a List with the original value
    # is desired.
    #
    # This is necessary, for example, in
    # ray._private.services.resolve_ip_for_localhost()
    #
    # Note: this does not handle IPv6 well at all.  For example parsing will
    #       fail with ::1 instead of 127.0.0.1.  For now, I don't think this
    #       needs to be handled.  From after this conditional, IPv6 will always
    #       be required to have ip:port or it will raise a ValueError.
    if ":" not in address:
        return [address]

    # do not attempt to parse if not ip:port or url-ish (*://ip:port/*) with
    # ip:port for DNS, IPv4, and IPv6 supported
    pattern = re.compile("^(.*/)?.*[^:]:[0-9]+(/.*)?$")
    if not pattern.match(address):
        raise ValueError(
            "Malformed address (expected address to "
            + "include IP or DNS host and port "
            + f"e.g. ip:port): {address}"
        )
    # use rsplit for IPv6 or IPv4
    ip, port = address.rsplit(":", 1)
    # check for url-ish
    if "/" in address:
        if "/" in ip:
            ip = re.sub(r"^.*/", r"", ip)
        if "/" in port:
            port = re.sub(r"^([^/]+)/.*", r"\g<1>", port)
    ip = re.sub(r"^\[?([^\]]*)\]?$", r"\g<1>", ip)
    return [ip, port]
