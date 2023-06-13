import logging
from typing import Callable

from ray.util.annotations import PublicAPI

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
        """Builds and wraps an ASGI app from the provided builder.

        The builder should take no arguments and return a Gradio App (of type Interface
        or Blocks).
        """
        io: Blocks = builder()
        super().__init__(routes.App.create_app(io))


GradioServer = serve.deployment(GradioIngress)
