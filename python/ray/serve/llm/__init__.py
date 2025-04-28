from typing import TYPE_CHECKING

from ray.util.annotations import PublicAPI


from ray.llm._internal.serve.configs.server_models import (
    LLMConfig as _LLMConfig,
    LLMServingArgs as _LLMServingArgs,
    ModelLoadingConfig as _ModelLoadingConfig,
    CloudMirrorConfig as _CloudMirrorConfig,
    LoraConfig as _LoraConfig,
)
from ray.llm._internal.serve.deployments.llm.llm_server import (
    LLMServer as _LLMServer,
)
from ray.llm._internal.serve.deployments.routers.router import (
    LLMRouter as _LLMRouter,
)


if TYPE_CHECKING:
    from ray.serve.deployment import Application


##########
# Models
##########


@PublicAPI(stability="alpha")
class LLMConfig(_LLMConfig):
    """The configuration for starting an LLM deployment."""

    pass


@PublicAPI(stability="alpha")
class LLMServingArgs(_LLMServingArgs):
    """The configuration for starting an LLM deployment application."""

    pass


@PublicAPI(stability="alpha")
class ModelLoadingConfig(_ModelLoadingConfig):
    """The configuration for loading an LLM model."""

    pass


@PublicAPI(stability="alpha")
class CloudMirrorConfig(_CloudMirrorConfig):
    """The configuration for mirroring an LLM model from cloud storage."""

    pass


@PublicAPI(stability="alpha")
class LoraConfig(_LoraConfig):
    """The configuration for loading an LLM model with LoRA."""

    pass


##########
# Builders
##########


@PublicAPI(stability="alpha")
def build_llm_deployment(llm_config: "LLMConfig") -> "Application":
    """Helper to build a single vllm deployment from the given llm config.

    Examples:
        .. testcode::
            :skipif: True

            from ray import serve
            from ray.serve.llm import LLMConfig, build_llm_deployment

            # Configure the model
            llm_config = LLMConfig(
                model_loading_config=dict(
                    model_id="llama-3.1-8b",
                    model_source="meta-llama/Llama-3.1-8b-instruct",
                ),
                deployment_config=dict(
                    autoscaling_config=dict(
                        min_replicas=1,
                        max_replicas=2,
                    )
                ),
                accelerator_type="A10G",
            )

            # Build the deployment
            llm_app = build_llm_deployment(llm_config)

            # Deploy the application
            model_handle = serve.run(llm_app)

            # Querying the model handle
            import asyncio
            model_handle = model_handle.options(stream=True)
            async def query_model(model_handle):
                from ray.serve.llm.openai_api_models import ChatCompletionRequest

                request = ChatCompletionRequest(
                    model="qwen-0.5b",
                    messages=[
                        {
                            "role": "user",
                            "content": "Hello, world!"
                        }
                    ]
                )

                resp = model_handle.chat.remote(request)
                async for message in resp:
                    print("message: ", message)

            asyncio.run(query_model(model_handle))

    Args:
        llm_config: The llm config to build vllm deployment.

    Returns:
        The configured Ray Serve Application for vllm deployment.
    """
    from ray.llm._internal.serve.builders import build_llm_deployment

    return build_llm_deployment(llm_config=llm_config)


@PublicAPI(stability="alpha")
def build_openai_app(llm_serving_args: "LLMServingArgs") -> "Application":
    """Helper to build an OpenAI compatible app with the llm deployment setup from
    the given llm serving args. This is the main entry point for users to create a
    Serve application serving LLMs.


    Examples:
        .. code-block:: python
            :caption: Example usage in code.

            from ray import serve
            from ray.serve.llm import LLMConfig, LLMServingArgs, build_openai_app

            llm_config1 = LLMConfig(
                model_loading_config=dict(
                    model_id="qwen-0.5b",
                    model_source="Qwen/Qwen2.5-0.5B-Instruct",
                ),
                deployment_config=dict(
                    autoscaling_config=dict(
                        min_replicas=1, max_replicas=2,
                    )
                ),
                accelerator_type="A10G",
            )

            llm_config2 = LLMConfig(
                model_loading_config=dict(
                    model_id="qwen-1.5b",
                    model_source="Qwen/Qwen2.5-1.5B-Instruct",
                ),
                deployment_config=dict(
                    autoscaling_config=dict(
                        min_replicas=1, max_replicas=2,
                    )
                ),
                accelerator_type="A10G",
            )

            # Deploy the application
            llm_app = build_openai_app(
                LLMServingArgs(
                    llm_configs=[
                        llm_config1,
                        llm_config2,
                    ]
                )
            )
            serve.run(llm_app)


            # Querying the model via openai client
            from openai import OpenAI

            # Initialize client
            client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

            # Basic completion
            response = client.chat.completions.create(
                model="qwen-0.5b",
                messages=[{"role": "user", "content": "Hello!"}]
            )

        .. code-block:: yaml
            :caption: Example usage in YAML.

            # config.yaml
            applications:
            - args:
                llm_configs:
                    - model_loading_config:
                        model_id: qwen-0.5b
                        model_source: Qwen/Qwen2.5-0.5B-Instruct
                      accelerator_type: A10G
                      deployment_config:
                        autoscaling_config:
                            min_replicas: 1
                            max_replicas: 2
                    - model_loading_config:
                        model_id: qwen-1.5b
                        model_source: Qwen/Qwen2.5-1.5B-Instruct
                      accelerator_type: A10G
                      deployment_config:
                        autoscaling_config:
                            min_replicas: 1
                            max_replicas: 2
              import_path: ray.serve.llm:build_openai_app
              name: llm_app
              route_prefix: "/"


    Args:
        llm_serving_args: The list of llm configs or the paths to the llm config to
            build the app.

    Returns:
        The configured Ray Serve Application router.
    """
    from ray.llm._internal.serve.builders import build_openai_app

    return build_openai_app(llm_serving_args=llm_serving_args)


#############
# Deployments
#############


@PublicAPI(stability="alpha")
class LLMServer(_LLMServer):
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
            from ray.serve.llm import LLMConfig, LLMServer, LLMRouter
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
            llm_app = LLMRouter.as_deployment().bind([vllm_deployment1, vllm_deployment2])
            serve.run(llm_app)
    """

    pass


__all__ = [
    "LLMConfig",
    "LLMServingArgs",
    "ModelLoadingConfig",
    "CloudMirrorConfig",
    "LoraConfig",
    "build_llm_deployment",
    "build_openai_app",
    "LLMServer",
    "LLMRouter",
]
