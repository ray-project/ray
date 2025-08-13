from ray.llm._internal.serve.deployments.llm.llm_server import (
    LLMServer as InternalLLMServer,
)

# TODO (Kourosh): Update the internal namespace.
from ray.llm._internal.serve.deployments.prefill_decode_disagg.prefill_decode_disagg import (
    PDProxyServer,
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
    through `as_deployment` method.

    Examples:
        .. testcode::
            :skipif: True

            from ray import serve
            from ray.serve.llm import LLMConfig, LLMServer

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
            LLMDeployment = LLMServer.as_deployment(llm_config.get_serve_options())
            llm_app = LLMDeployment.bind(llm_config)

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
class PDServer(PDProxyServer):
    """A server for prefill-decode disaggregation.

    This server acts as a proxy in a prefill-decode disaggregated system.
    For chat and completions, proxy sends the request to the prefill server
    with max_tokens=1 and then sends the returned metadata to the decode server.

    Args:
        prefill_server: The prefill server deployment handle.
        decode_server: The decode server deployment handle.
    """

    pass
