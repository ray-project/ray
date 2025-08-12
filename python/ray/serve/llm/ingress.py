from ray.llm._internal.serve.deployments.routers.router import (
    LLMRouter as _LLMRouter,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class OpenAiIngress(_LLMRouter):

    """The implementation of the OpenAI compatiple model router.

    This deployment creates the following endpoints:
      - /v1/chat/completions: Chat interface (OpenAI-style)
      - /v1/completions: Text completion
      - /v1/models: List available models
      - /v1/models/{model}: Model information


    Examples:
        .. testcode::
            :skipif: True


            from ray import serve
            from ray.serve.llm import LLMConfig
            from ray.serve.llm.deployment import LLMServer
            from ray.serve.llm.ingress import OpenAiIngress
            from ray.serve.llm.openai_api_models import ChatCompletionRequest


            llm_config1 = LLMConfig(
                model_loading_config=dict(
                    served_model_name="llama-3.1-8b",  # Name shown in /v1/models
                    model_source="meta-llama/Llama-3.1-8b-instruct",
                ),
                deployment_config=dict(
                    autoscaling_config=dict(
                        min_replicas=1, max_replicas=8,
                    )
                ),
            )
            llm_config2 = LLMConfig(
                model_loading_config=dict(
                    served_model_name="llama-3.2-3b",  # Name shown in /v1/models
                    model_source="meta-llama/Llama-3.2-3b-instruct",
                ),
                deployment_config=dict(
                    autoscaling_config=dict(
                        min_replicas=1, max_replicas=8,
                    )
                ),
            )

            # Deploy the application
            vllm_deployment1 = LLMServer.as_deployment(llm_config1.get_serve_options()).bind(llm_config1)
            vllm_deployment2 = LLMServer.as_deployment(llm_config2.get_serve_options()).bind(llm_config2)
            llm_app = OpenAiIngress.as_deployment().bind([vllm_deployment1, vllm_deployment2])
            serve.run(llm_app)
    """

    pass
