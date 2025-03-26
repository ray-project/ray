import logging
from typing import Callable

from ray import serve
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.http_util import ASGIAppReplicaWrapper
from ray.util.annotations import PublicAPI

try:
    from gradio import Blocks, routes
except ModuleNotFoundError:
    print("Gradio isn't installed. Run `pip install gradio` to install Gradio.")
    raise

logger = logging.getLogger(SERVE_LOGGER_NAME)


@PublicAPI(stability="alpha")
class GradioIngress(ASGIAppReplicaWrapper):
    """User-facing class that wraps a Gradio App in a Serve Deployment."""

    def __init__(self, builder: Callable[[], Blocks]):
        """Builds and wraps an ASGI app from the provided builder.

        The builder should take no arguments and return a Gradio App (of type Interface
        or Blocks).
        """
        io: Blocks = builder()
        super().__init__(routes.App.create_app(io))


GradioServer = serve.deployment(GradioIngress)
