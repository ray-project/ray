from typing import TYPE_CHECKING, Optional

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.serve.deployment import Application


@PublicAPI(stability="alpha")
def build_mx_app(mx_args: Optional[dict] = None) -> "Application":
    """Build the ModelExpress (MX) server app.

    LLM deployments with ``engine_kwargs={"load_format": "modelexpress"}`` load
    their weights and JIT caches from an already-serving replica over NIXL. They
    find each other through this app, which runs the MX server on the head
    node. Deploy it as its own app named ``mx`` (or set ``RAY_MX_APP_NAME`` on
    the LLM apps) with ``route_prefix=None``.

    The server binary ships in the ray-llm image; set
    ``RAY_MX_SERVER_BINARY_PATH`` to use another one.

    Examples:
        .. testcode::
            :skipif: True

            from ray import serve
            from ray.serve.llm import LLMConfig, build_openai_app
            from ray.serve.llm.mx import build_mx_app

            serve.run(build_mx_app(), name="mx", route_prefix=None)

            llm_config = LLMConfig(
                model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"),
                deployment_config=dict(
                    autoscaling_config=dict(min_replicas=1, max_replicas=4),
                ),
                engine_kwargs=dict(load_format="modelexpress"),
            )
            serve.run(build_openai_app({"llm_configs": [llm_config]}), name="llm")

    Args:
        mx_args: Server options:

            - ``metadata_backend``: ``"memory"`` (default) or ``"redis"``.
              For ``"redis"``, set ``REDIS_URL`` in the app's ``runtime_env``.
            - ``startup_timeout_s``: How long to wait for the server to start.
              Defaults to 30.

    Returns:
        The MX server application.
    """
    from ray.llm._internal.serve.serving_patterns.model_express.builder import (
        build_model_express_app,
    )

    return build_model_express_app(builder_config=mx_args)


__all__ = ["build_mx_app"]
