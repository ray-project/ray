from concurrent.futures import ThreadPoolExecutor, TimeoutError
from io import BytesIO
import ipaddress
import socket
import urllib.parse
import PIL
from PIL import Image
import requests
import starlette.requests
import torch
import torchvision.models as models
from torchvision.models import ResNet50_Weights
from torchvision import transforms

from ray import serve


@serve.deployment
class Model:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.resnet50 = (
            models.resnet50(weights=ResNet50_Weights.DEFAULT).eval().to(self.device)
        )
        self.preprocess = transforms.Compose(
            [
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                ),
            ]
        )
        with open("imagenet_classes.txt", "r") as f:
            self.categories = [s.strip() for s in f.readlines()]

        self.model_thread_pool = ThreadPoolExecutor(max_workers=5)

    async def __call__(self, request: starlette.requests.Request) -> str:
        uri = (await request.json())["uri"]

        # Validate URI scheme and host to prevent SSRF
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme not in ("http", "https"):
            return
        hostname = parsed.hostname or ""
        if not hostname:
            return

        # Reject literal private/loopback/link-local/reserved IP addresses
        try:
            addr = ipaddress.ip_address(hostname)
            if (
                addr.is_private
                or addr.is_loopback
                or addr.is_link_local
                or addr.is_reserved
            ):
                return
        except ValueError:
            pass  # Not a literal IP; validated via DNS resolution below

        # Resolve the hostname and validate every returned IP to prevent SSRF
        # via DNS rebinding or domains that resolve to internal addresses
        # (e.g. attacker-controlled domains, nip.io-style services, etc.)
        try:
            resolved = socket.getaddrinfo(hostname, None)
            for result in resolved:
                ip_str = result[4][0]
                addr = ipaddress.ip_address(ip_str)
                if (
                    addr.is_private
                    or addr.is_loopback
                    or addr.is_link_local
                    or addr.is_reserved
                ):
                    return
        except (socket.gaierror, ValueError):
            # Reject unresolvable hostnames rather than allowing them through
            return

        try:
            # Disable redirect following to prevent SSRF bypass via a redirect
            # from an external URL to an internal resource
            image_bytes = requests.get(
                uri, timeout=5, allow_redirects=False
            ).content
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.Timeout,
        ):
            return

        try:
            image = Image.open(BytesIO(image_bytes)).convert("RGB")
        except PIL.UnidentifiedImageError:
            return

        images = [image]  # Batch size is 1

        def run_model():
            input_tensor = torch.cat(
                [self.preprocess(img).unsqueeze(0) for img in images]
            ).to(self.device)
            with torch.no_grad():
                output = self.resnet50(input_tensor)
                sm_output = torch.nn.functional.softmax(output[0], dim=0)
            return torch.argmax(sm_output)

        try:
            future = self.model_thread_pool.submit(run_model)
            ind = future.result(timeout=5)
            return self.categories[ind]
        except TimeoutError:
            return


app = Model.bind()
