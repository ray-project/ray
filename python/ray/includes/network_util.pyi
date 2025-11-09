# source: network_util.pxi
from typing import Optional, Tuple, Union

def parse_address(address: str) -> Optional[Tuple[str, str]]:
    ...

def build_address(host: str, port: Union[int, str]) -> str:
    ...

def node_ip_address_from_perspective(address: Optional[str] = None) -> str:
    ...

def is_ipv6(host: str) -> bool:
    ...
