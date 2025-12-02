from typing import TYPE_CHECKING, Optional, Type

from ray._common.deprecation import Deprecated
from ray.llm._internal.serve.core.configs.llm_config import (
    CloudMirrorConfig as _CloudMirrorConfig,
    LLMConfig as _LLMConfig,
    LoraConfig as _LoraConfig,
    ModelLoadingConfig as _ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs as _LLMServingArgs,
)
from ray.llm._internal.serve.core.ingress.ingress import (
    OpenAiIngress as _OpenAiIngress,
)

# For backward compatibility
from ray.llm._internal.serve.core.server.llm_server import (
    LLMServer as _LLMServer,
)
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
class LLMRouter(_OpenAiIngress):
    pass


##########
# Builders
##########


@PublicAPI(stability="alpha")
def build_llm_deployment(
    llm_config: "LLMConfig",
    *,
    name_prefix: Optional[str] = None,
    bind_kwargs: Optional[dict] = None,
    override_serve_options: Optional[dict] = None,
    deployment_cls: Optional[Type[LLMServer]] = None,
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
        bind_kwargs: Optional kwargs to pass to the deployment.
        override_serve_options: Optional serve options to override the original serve options based on the llm_config.
        deployment_cls: Optional deployment class to use.

    Returns:
        The configured Ray Serve Application for vllm deployment.
    """
    from ray.llm._internal.serve.core.server.builder import (
        build_llm_deployment,
    )

    return build_llm_deployment(
        llm_config=llm_config,
        name_prefix=name_prefix,
        bind_kwargs=bind_kwargs,
        override_serve_options=override_serve_options,
        deployment_cls=deployment_cls,
    )


@PublicAPI(stability="alpha")
def build_openai_app(llm_serving_args: dict) -> "Application":
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
        llm_serving_args: A dict that conforms to the LLMServingArgs pydantic model.

    Returns:
        The configured Ray Serve Application router.
    """
    from ray.llm._internal.serve.core.ingress.builder import (
        build_openai_app,
    )

    return build_openai_app(builder_config=llm_serving_args)


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
        pd_serving_args: The dictionary containing prefill and decode configs. See PDServingArgs for more details.

    Returns:
        The configured Ray Serve Application router.
    """
    from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
        build_pd_openai_app,
    )

    return build_pd_openai_app(pd_serving_args=pd_serving_args)


@PublicAPI(stability="alpha")
def build_dp_deployment(
    llm_config: "LLMConfig",
    *,
    name_prefix: Optional[str] = None,
    override_serve_options: Optional[dict] = None,
) -> "Application":
    """Build a data parallel attention LLM deployment.

    Args:
        llm_config: The LLM configuration.
        name_prefix: The prefix to add to the deployment name.
        override_serve_options: The optional serve options to override the
            default options.

    Returns:
        The Ray Serve Application for the data parallel attention LLM deployment.
    """
    from ray.llm._internal.serve.serving_patterns.data_parallel.builder import (
        build_dp_deployment,
    )

    return build_dp_deployment(
        llm_config=llm_config,
        name_prefix=name_prefix,
        override_serve_options=override_serve_options,
    )


@PublicAPI(stability="alpha")
def build_dp_openai_app(dp_serving_args: dict) -> "Application":
    """Build an OpenAI compatible app with the DP attention deployment
    setup from the given builder configuration.

    Args:
        dp_serving_args: The configuration for the builder. It has to conform
            to the DPOpenAiServingArgs pydantic model.

    Returns:
        The configured Ray Serve Application.
    """
    from ray.llm._internal.serve.serving_patterns.data_parallel.builder import (
        build_dp_openai_app,
    )

    return build_dp_openai_app(builder_config=dp_serving_args)


__all__ = [
    "LLMConfig",
    "LLMServingArgs",
    "ModelLoadingConfig",
    "CloudMirrorConfig",
    "LoraConfig",
    "build_llm_deployment",
    "build_openai_app",
    "build_pd_openai_app",
    "build_dp_deployment",
    "build_dp_openai_app",
    "LLMServer",
    "LLMRouter",
]
