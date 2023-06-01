import asyncio
import logging
from typing import Callable

from ray.util.annotations import PublicAPI
from ray._private.utils import get_or_create_event_loop

from ray import serve
from ray.serve._private.http_util import (
    ASGIAppReplicaWrapper,
)

try:
    from gradio import routes, Blocks
except ModuleNotFoundError:
    print("Gradio isn't installed. Run `pip install gradio` to install Gradio.")
    raise

logger = logging.getLogger(__file__)


@PublicAPI(stability="alpha")
class GradioIngress(ASGIAppReplicaWrapper):
    """User-facing class that wraps a Gradio App in a Serve Deployment."""

    def __init__(self, builder: Callable[[], Blocks]):
        """
        Takes a builder function which should take no arguments and return the Gradio
        App (of type Interface or Blocks) to deploy as a Serve Deployment.
        """
        io: Blocks = builder()

        # ASGIAppReplicaWrapper's constructor is `async def` because it calls the ASGI
        # LifespanOn event, but we want this constructor to be sync because it's a
        # public API.
        asyncio.run_coroutine_threadsafe(
            super().__init__(routes.App.create_app(io)),
            get_or_create_event_loop()
        ).result()


GradioServer = serve.deployment(GradioIngress)
