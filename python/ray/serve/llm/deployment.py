from ray.llm._internal.serve.core.server.llm_server import (
    LLMServer as InternalLLMServer,
)
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import (
    DPServer as _DPServer,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server import (
    PDProxyServer as _PDProxyServer,
)
from ray.util.annotations import PublicAPI

#############
# Deployments
#############


@PublicAPI(stability="alpha")
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


@PublicAPI(stability="alpha")
class PDProxyServer(_PDProxyServer):
    """A proxy server for prefill-decode disaggregation.

    This server acts as a proxy in a prefill-decode disaggregated system.
    For chat and completions, proxy sends the request to the prefill server
    with max_tokens=1 and then sends the returned metadata to the decode server.

    Args:
        prefill_server: The prefill server deployment handle.
        decode_server: The decode server deployment handle.
    """

    pass


@PublicAPI(stability="alpha")
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


__all__ = ["LLMServer", "PDProxyServer", "DPServer"]
