from typing import Optional

from ray import serve
from ray.llm._internal.serve.serving_patterns.model_express.mx_server import (
    ModelExpressServer,
)


def build_model_express_app(builder_config: Optional[dict] = None) -> serve.Application:
    """Build the `mx` app. Deploy it with ``route_prefix=None``.

    Takes one dict so it also works as a Serve config ``import_path`` builder.

    Args:
        builder_config: Keyword arguments for ``ModelExpressServer``.

    Returns:
        The `mx` application.
    """
    return ModelExpressServer.bind(**(builder_config or {}))
