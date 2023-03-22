from ray import serve
from ray.serve._private.http_util import ASGIHTTPSender
from ray.util.annotations import PublicAPI

from starlette import requests
from typing import Callable

try:
    from gradio import routes, Blocks
except ModuleNotFoundError:
    print("Gradio isn't installed. Run `pip install gradio` to install Gradio.")
    raise


@PublicAPI(stability="alpha")
class GradioIngress:
    """User-facing class that wraps a Gradio App in a Serve Deployment."""

    def __init__(self, builder: Callable[[], Blocks]):
        """
        Takes a builder function which should take no arguments and return the Gradio
        App (of type Interface or Blocks) to deploy as a Serve Deployment.
        """
        io: Blocks = builder()
        self.app = routes.App.create_app(io)

    async def __call__(self, request: requests.Request):
        sender = ASGIHTTPSender()
        await self.app(request.scope, receive=request.receive, send=sender)
        return sender.build_asgi_response()


GradioServer = serve.deployment(GradioIngress)
