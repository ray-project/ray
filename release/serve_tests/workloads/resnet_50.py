from concurrent.futures import ThreadPoolExecutor, TimeoutError
from io import BytesIO
import ipaddress
import socket
import urllib.parse
import urllib3
import PIL
from PIL import Image
import starlette.requests
import torch
import torchvision.models as models
from torchvision.models import ResNet50_Weights
import torchvision.transforms as transforms

from ray import serve


# Carrier-Grade NAT / Shared Address Space (RFC 6598).
# Python's ipaddress module does NOT flag 100.64.0.0/10 as private
# (CPython issue #119812), so we define it explicitly for the check below.
_CGN_NETWORK = ipaddress.ip_network("100.64.0.0/10")


def _is_safe_ip(ip_str: str) -> bool:
    """Return True only if *ip_str* is a globally-routable, public IP address."""
    try:
        addr = ipaddress.ip_address(ip_str)
        if (
            addr.is_private
            or addr.is_loopback
            or addr.is_link_local
            or addr.is_reserved
        ):
            return False
        # Explicitly block Carrier-Grade NAT range not caught by the flags above.
        if addr in _CGN_NETWORK:
            return False
        return True
    except ValueError:
        return False


def _fetch_image_ssrf_safe(uri: str):
    """
    Fetch *uri* and return its raw content, or ``None`` if rejected.

    SSRF protection strategy
    ------------------------
    1.  Validate the URL scheme (must be http or https).
    2.  Resolve the hostname **once** via ``socket.getaddrinfo``.
    3.  Validate *every* resolved IP against private/reserved/CGN ranges.
    4.  Connect **directly to the validated IP** so that urllib3 never
        performs a second DNS lookup — eliminating the TOCTOU window that
        exists when validation and the actual connection are separate steps.
    5.  For HTTPS, pass the original hostname as ``server_hostname`` so
        that TLS SNI and certificate verification still use the correct name.
    6.  Disable redirect following (``redirect=False``) to prevent an
        external URL from redirecting the connection to an internal resource.
    """
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme not in ("http", "https"):
        return None
    hostname = parsed.hostname or ""
    if not hostname:
        return None

    port = parsed.port or (443 if parsed.scheme == "https" else 80)

    # --- Single DNS resolution + full IP validation ---
    try:
        resolved = socket.getaddrinfo(hostname, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        # Reject unresolvable hostnames rather than allowing them through.
        return None

    if not resolved:
        return None

    safe_ip = None
    for _family, _type, _proto, _canonname, sockaddr in resolved:
        ip_str = sockaddr[0]
        if not _is_safe_ip(ip_str):
            # Any resolved IP being internal means the whole request is rejected.
            return None
        if safe_ip is None:
            safe_ip = ip_str  # remember the first validated IP

    if safe_ip is None:
        return None

    # --- Connect directly to the validated IP (no second DNS lookup) ---
    # Reconstruct path including semicolon-delimited path parameters (parsed.params)
    # so they are not silently dropped (urllib.parse.urlparse splits them out).
    path = parsed.path or "/"
    if parsed.params:
        path += ";" + parsed.params
    if parsed.query:
        path += "?" + parsed.query

    # Preserve the original Host header for correct virtual-host routing.
    # Per RFC 7230, include the port when it is non-default, and wrap IPv6
    # addresses in square brackets (parsed.hostname strips them).
    default_port = 443 if parsed.scheme == "https" else 80
    try:
        # Detect IPv6 by attempting to parse as an IP address.
        addr_obj = ipaddress.ip_address(hostname)
        if addr_obj.version == 6:
            bare = f"[{hostname}]"
        else:
            bare = hostname
    except ValueError:
        bare = hostname
    host_header = f"{bare}:{port}" if port != default_port else bare
    headers = {"Host": host_header}

    timeout = urllib3.Timeout(connect=5, read=5)
    try:
        if parsed.scheme == "https":
            # server_hostname keeps SNI and certificate verification working
            # against the original domain name even though we connect to an IP.
            pool = urllib3.HTTPSConnectionPool(
                safe_ip,
                port=port,
                server_hostname=hostname,
                timeout=timeout,
            )
        else:
            pool = urllib3.HTTPConnectionPool(safe_ip, port=port, timeout=timeout)

        # Use the pool as a context manager to ensure the underlying socket
        # resources are released promptly rather than relying on GC.
        with pool:
            response = pool.request("GET", path, headers=headers, redirect=False)
            return response.data
    except urllib3.exceptions.HTTPError:
        return None


@serve.deployment
class Model:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = (
            models.resnet50(weights=ResNet50_Weights.DEFAULT).eval().to(self.device)
        )
        self.preprocessor = transforms.Compose(
            [
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                ),
            ]
        )

    async def __call__(self, request: starlette.requests.Request) -> str:
        uri = (await request.json())["uri"]

        image_bytes = _fetch_image_ssrf_safe(uri)
        if image_bytes is None:
            return

        try:
            image = self.preprocessor(Image.open(BytesIO(image_bytes)).convert("RGB"))
        except Exception:
            return

        with torch.no_grad():
            output = self.model(torch.stack([image]).to(self.device))

        return str(torch.argmax(output, dim=1).tolist())


app = Model.bind()
