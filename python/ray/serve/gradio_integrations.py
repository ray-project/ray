import logging
from typing import Callable, Optional

from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Send
from uvicorn.config import Config
from uvicorn.lifespan.on import LifespanOn

from ray.util.annotations import PublicAPI
from ray._private.utils import get_or_create_event_loop

from ray import serve
from ray.serve._private.http_util import ASGIHTTPSender
from ray.serve._private.logging_utils import LoggingContext

try:
    from gradio import routes, Blocks
except ModuleNotFoundError:
    print("Gradio isn't installed. Run `pip install gradio` to install Gradio.")
    raise

logger = logging.getLogger(__file__)


@PublicAPI(stability="alpha")
class GradioIngress:
    """User-facing class that wraps a Gradio App in a Serve Deployment."""

    def __init__(self, builder: Callable[[], Blocks]):
        """
        Takes a builder function which should take no arguments and return the Gradio
        App (of type Interface or Blocks) to deploy as a Serve Deployment.
        """
        # Used in `replica.py` to detect the usage of this class.
        self._is_serve_asgi_wrapper = True

        io: Blocks = builder()
        self.app = routes.App.create_app(io)

        # Use uvicorn's lifespan handling code to properly deal with
        # startup and shutdown event.
        self._serve_asgi_lifespan = LifespanOn(
            Config(self.app, lifespan="on")
        )
        # Replace uvicorn logger with our own.
        self._serve_asgi_lifespan.logger = logger

        get_or_create_event_loop().create_task(self._serve_asgi_lifespan.startup())

    async def __call__(
        self,
        request: Request,
        asgi_sender: Optional[Send] = None,
        asgi_receive: Optional[Receive] = None,
    ) -> Optional[ASGIApp]:
        """Calls into the wrapped ASGI app.

        If asgi_sender is provided, it's passed into the app and nothing is
        returned.

        If no asgi_sender is provided, an ASGI response is built and returned.
        """
        build_and_return_response = False
        if asgi_sender is None:
            asgi_sender = ASGIHTTPSender()
            build_and_return_response = True

        if asgi_receive is None:
            print("USING request.receive")
            asgi_receive = request.receive

        if asgi_sender is None:
            scope = request.scope
        else:
            scope = request

        await self.app(
            scope,
            asgi_receive,
            asgi_sender,
        )

        if build_and_return_response:
            return asgi_sender.build_asgi_response()


GradioServer = serve.deployment(GradioIngress)
