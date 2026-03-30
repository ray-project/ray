import warnings

from ray.llm._internal.serve.core.server.llm_server import (
    LLMServer as InternalLLMServer,
)
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import (
    DPServer as _DPServer,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server import (
    PDDecodeServer as _PDDecodeServer,
    PDPrefillServer as _PDPrefillServer,
    PDProxyServer as _PDProxyServer,  # TODO(Kourosh): Remove in Ray 2.56.
)
from ray.util.annotations import PublicAPI

#############
# Deployments
#############


@PublicAPI(stability="beta")
class LLMServer(InternalLLMServer):
    """The implementation of the vLLM engine deployment.

    To build a Deployment object you should use `build_llm_deployment` function.
    We also expose a lower level API for more control over the deployment class
    through `serve.deployment` function.

    Examples:
        .. testcode::
            :skipif: True

            from ray import serve
            from ray.serve.llm import LLMConfig
            from ray.serve.llm.deployment import LLMServer

            # Configure the model
            llm_config = LLMConfig(
                model_loading_config=dict(
                    served_model_name="llama-3.1-8b",
                    model_source="meta-llama/Llama-3.1-8b-instruct",
                ),
                deployment_config=dict(
                    autoscaling_config=dict(
                        min_replicas=1,
                        max_replicas=8,
                    )
                ),
            )

            # Build the deployment directly
            serve_options = LLMServer.get_deployment_options(llm_config)
            llm_app = serve.deployment(LLMServer).options(
                **serve_options).bind(llm_config)

            model_handle = serve.run(llm_app)

            # Query the model via `chat` api
            from ray.serve.llm.openai_api_models import ChatCompletionRequest
            request = ChatCompletionRequest(
                model="llama-3.1-8b",
                messages=[
                    {
                        "role": "user",
                        "content": "Hello, world!"
                    }
                ]
            )
            response = ray.get(model_handle.chat(request))
            print(response)
    """

    pass


@PublicAPI(stability="beta")
class PDDecodeServer(_PDDecodeServer):
    """Decode-side LLM server for prefill-decode disaggregation.

    This deployment owns a real engine (decode config) and holds a handle
    to the prefill deployment. For chat/completions it runs remote prefill
    first, then local decode.

    Use ``build_pd_openai_app`` to construct the full 3-tier PD graph.
    """

    pass


@PublicAPI(stability="beta")
class PDPrefillServer(_PDPrefillServer):
    """Prefill-side LLM server for prefill-decode disaggregation.

    A standard LLMServer with an additional ``prewarm_prefill`` method
    used during the optional pre-warm handshake.
    """

    pass


# TODO(Kourosh): Remove in Ray 2.56.
class PDProxyServer(_PDProxyServer):
    """A proxy server for prefill-decode disaggregation.

    .. deprecated::
        ``PDProxyServer`` is deprecated. Use ``PDDecodeServer`` instead.
        This class will be removed in a future release.
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        warnings.warn(
            "PDProxyServer is deprecated. Use PDDecodeServer instead.",
            DeprecationWarning,
            stacklevel=2,
        )


@PublicAPI(stability="beta")
class DPServer(_DPServer):
    """Data Parallel LLM Server.

    This class is used to serve data parallel attention (DP Attention)
    deployment paradigm, where the attention layers are replicated and
    the MoE layers are sharded. DP Attention is typically used for models
    like DeepSeek-V3.

    To build a Deployment object you should use `build_dp_deployment` function.
    We also expose a lower level API for more control over the deployment class
    through `serve.deployment` function.

    Examples:
        .. testcode::
            :skipif: True

            from ray import serve
            from ray.serve.llm import LLMConfig, build_dp_deployment

            # Configure the model
            llm_config = LLMConfig(
                model_loading_config=dict(
                    model_id="Qwen/Qwen2.5-0.5B-Instruct",
                ),
                engine_kwargs=dict(
                    data_parallel_size=2,
                    tensor_parallel_size=1,
                ),
                experimental_configs=dict(
                    dp_size_per_node=2,
                ),
                accelerator_type="A10G",
            )

            # Build the deployment
            dp_app = build_dp_deployment(llm_config)

            # Deploy the application
            model_handle = serve.run(dp_app)
    """

    pass


__all__ = [
    "LLMServer",
    "PDDecodeServer",
    "PDPrefillServer",
    "PDProxyServer",  # TODO(Kourosh): Remove in Ray 2.56.
    "DPServer",
]
