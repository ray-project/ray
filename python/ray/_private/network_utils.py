from typing import Tuple, Union


def parse_address(address: str) -> Tuple[str, str]:
    host, port = address.rsplit(":", 1)
    host = host.strip("[]")
    return (host, port)


def build_address(host: str, port: Union[int, str]) -> str:
    port_str = str(port)

    # host is IPv6
    if ":" in host:
        return f"[{host}]:{port_str}"
    # host is IPv4 or a hostname
    else:
        return f"{host}:{port_str}"
