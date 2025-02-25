from ray.llm._internal.serve.deployments.llm.vllm.vllm_deployment import (
    VLLMService as _VLLMService,
)
from ray.llm._internal.serve.deployments.routers.router import (
    LLMRouter as _LLMRouter,
)


from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class VLLMService(_VLLMService):
    """The implementation of the VLLM engine deployment.

    To build a VLLMDeployment object you should use `build_vllm_deployment` function.
    We also expose a lower level API for more control over the deployment class
    through `as_deployment` method.

    Examples:
        .. testcode::
            :skipif: True

            from ray import serve
            from ray.serve.config import AutoscalingConfig
            from ray.serve.llm.configs import LLMConfig, ModelLoadingConfig, DeploymentConfig
            from ray.serve.llm.deployments import VLLMDeployment
            from ray.serve.llm.openai_api_models import ChatCompletionRequest

            # Configure the model
            llm_config = LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    served_model_name="llama-3.1-8b",
                    model_source="meta-llama/Llama-3.1-8b-instruct",
                ),
                deployment_config=DeploymentConfig(
                    autoscaling_config=AutoscalingConfig(
                        min_replicas=1,
                        max_replicas=8,
                    )
                ),
            )

            # Build the deployment directly
            VLLMDeployment = VLLMService.as_deployment(llm_config.get_serve_options())
            vllm_app = VLLMDeployment.bind(llm_config)

            model_handle = serve.run(vllm_app)

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
class LLMRouter(_LLMRouter):

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
            from ray.serve.config import AutoscalingConfig
            from ray.serve.llm.configs import LLMConfig, ModelLoadingConfig, DeploymentConfig
            from ray.serve.llm.deployments import VLLMDeployment
            from ray.serve.llm.openai_api_models import ChatCompletionRequest


            llm_config1 = LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    served_model_name="llama-3.1-8b",  # Name shown in /v1/models
                    model_source="meta-llama/Llama-3.1-8b-instruct",
                ),
                deployment_config=DeploymentConfig(
                    autoscaling_config=AutoscalingConfig(
                        min_replicas=1, max_replicas=8,
                    )
                ),
            )
            llm_config2 = LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    served_model_name="llama-3.2-3b",  # Name shown in /v1/models
                    model_source="meta-llama/Llama-3.2-3b-instruct",
                ),
                deployment_config=DeploymentConfig(
                    autoscaling_config=AutoscalingConfig(
                        min_replicas=1, max_replicas=8,
                    )
                ),
            )

            # Deploy the application
            vllm_deployment1 = VLLMDeployment.as_deployment(llm_config1.get_serve_options()).bind(llm_config1)
            vllm_deployment2 = VLLMDeployment.as_deployment(llm_config2.get_serve_options()).bind(llm_config2)
            llm_app = LLMModelRouterDeployment.as_deployment().bind([vllm_deployment1, vllm_deployment2])
            serve.run(llm_app)
    """

    pass
