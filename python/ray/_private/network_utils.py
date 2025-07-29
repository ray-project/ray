import ipaddress

def is_ipv6_address(ip):
    if ip is None:
        return False
    if len(ip) >= 2 and ip[0] == "[":
        ip = ip[1:-1]
    try:
        ip_obj = ipaddress.ip_address(ip)
        return ip_obj.version == 6
    except ValueError:
        return False

def parse_address(address):
    host, port = address.rsplit(":", 1)
    host = host.strip("[]")
    return (host, port)

def build_address(host, port):
    if ':' in host:
        return f"[{host}]:{port}"
    else:
        return f"{host}:{port}"
