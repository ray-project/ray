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


@PublicAPI(stability="alpha")
class GradioIngress(ASGIAppReplicaWrapper):
    """User-facing class that wraps a Gradio App in a Serve Deployment."""

    async def __init__(self, builder: Callable[[], Blocks]):
        """
        Takes a builder function which should take no arguments and return the Gradio
        App (of type Interface or Blocks) to deploy as a Serve Deployment.
        """
        io: Blocks = builder()
        await super().__init__(routes.App.create_app(io))


GradioServer = serve.deployment(GradioIngress)
