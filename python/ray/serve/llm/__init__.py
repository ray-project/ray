from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from ray.llm._internal.serve.configs.server_models import (
    CloudMirrorConfig as _CloudMirrorConfig,
    LLMConfig as _LLMConfig,
    LLMServingArgs as _LLMServingArgs,
    LoraConfig as _LoraConfig,
    ModelLoadingConfig as _ModelLoadingConfig,
)

# For backward compatibility
from ray.llm._internal.serve.deployments.llm.llm_server import (
    LLMServer as _LLMServer,
)
from ray.llm._internal.serve.deployments.routers.router import (
    LLMRouter as _LLMRouter,
)

# Using Deprecated from rllib since they are retuning better messages.
# TODO: Ray core should inherit that.
from ray.rllib.utils.deprecation import Deprecated
from ray.util.annotations import PublicAPI

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


#############
# Deployments
#############


@Deprecated(
    old="ray.serve.llm.LLMServer", new="ray.serve.llm.deployment.LLMServer", error=False
)
class LLMServer(_LLMServer):
    pass


@Deprecated(
    old="ray.serve.llm.LLMRouter",
    new="ray.serve.llm.ingress.OpenAIIngress",
    error=False,
)
class LLMRouter(_LLMRouter):
    pass


##########
# Builders
##########


@PublicAPI(stability="alpha")
def build_llm_deployment(
    llm_config: "LLMConfig", *, name_prefix: Optional[str] = None
) -> "Application":
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
        name_prefix: Optional prefix to be used for the deployment name.

    Returns:
        The configured Ray Serve Application for vllm deployment.
    """
    from ray.llm._internal.serve.builders import build_llm_deployment

    return build_llm_deployment(llm_config=llm_config, name_prefix=name_prefix)


@PublicAPI(stability="alpha")
def build_openai_app(
    llm_serving_args: Union["LLMServingArgs", Dict[str, Any]]
) -> "Application":
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
        llm_serving_args: Either a dict with "llm_configs" key containing a list of
            LLMConfig objects, or an LLMServingArgs object.

    Returns:
        The configured Ray Serve Application router.
    """
    from ray.llm._internal.serve.builders import build_openai_app

    return build_openai_app(llm_serving_args=llm_serving_args)


@PublicAPI(stability="alpha")
def build_pd_openai_app(pd_serving_args: dict) -> "Application":
    """Build a deployable application utilizing P/D disaggregation.


    Examples:
        .. code-block:: python
            :caption: Example usage in code.

            from ray import serve
            from ray.serve.llm import LLMConfig, build_pd_openai_app

            config = LLMConfig(
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

            # Deploy the application
            llm_app = build_pd_openai_app(
                dict(
                    prefill_config=config,
                    decode_config=config,
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
                prefill_config:
                    model_loading_config:
                        model_id: qwen-0.5b
                        model_source: Qwen/Qwen2.5-0.5B-Instruct
                    accelerator_type: A10G
                    deployment_config:
                        autoscaling_config:
                            min_replicas: 1
                            max_replicas: 2
                decode_config:
                    model_loading_config:
                    model_id: qwen-1.5b
                    model_source: Qwen/Qwen2.5-1.5B-Instruct
                    accelerator_type: A10G
                    deployment_config:
                    autoscaling_config:
                        min_replicas: 1
                        max_replicas: 2
              import_path: ray.serve.llm:build_pd_openai_app
              name: llm_app
              route_prefix: "/"


    Args:
        pd_serving_args: The dictionary containing prefill and decode configs.

    Returns:
        The configured Ray Serve Application router.
    """
    from ray.llm._internal.serve.deployments.prefill_decode_disagg.prefill_decode_disagg import (
        build_pd_openai_app as _build_pd_openai_app,
    )

    return _build_pd_openai_app(pd_serving_args=pd_serving_args)


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
