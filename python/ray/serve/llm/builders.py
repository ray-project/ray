from typing import TYPE_CHECKING

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.serve.deployment import Application
    from ray.serve.llm.configs import LLMConfig, LLMServingArgs


@PublicAPI(stability="alpha")
def build_vllm_deployment(llm_config: "LLMConfig") -> "Application":
    """Helper to build a single vllm deployment from the given llm config.

    Examples:
        .. testcode::
            :skipif: True

            from ray import serve
            from ray.serve.llm.configs import LLMConfig
            from ray.serve.llm.builders import build_vllm_deployment

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
            vllm_app = build_vllm_deployment(llm_config)

            # Deploy the application
            serve.run(vllm_app)

    Args:
        llm_config: The llm config to build vllm deployment.

    Returns:
        The configured Ray Serve Application for vllm deployment.
    """
    from ray.llm._internal.serve.builders import build_vllm_deployment

    return build_vllm_deployment(llm_config=llm_config)


@PublicAPI(stability="alpha")
def build_openai_app(llm_serving_args: "LLMServingArgs") -> "Application":
    """Helper to build an OpenAI compatible app with the llm deployment setup from
    the given llm serving args. This is the main entry point for users to create a
    Serve application serving LLMs.


    Examples:
        .. testcode::
            :skipif: True

            from ray import serve
            from ray.serve.llm.configs import LLMConfig
            from ray.serve.llm.builders import build_openai_app

            # Configure multiple models
            llm_config1 = LLMConfig(
                model_loading_config=dict(
                    model_id="llama-3.1-8b",
                    model_source="meta-llama/Llama-3.1-8b-instruct",
                ),
                deployment_config=dict(
                    autoscaling_config=dict(
                        min_replicas=1,
                        max_replicas=8,
                    )
                ),
            )

            llm_config2 = LLMConfig(
                model_loading_config=dict(
                    model_id="llama-3.2-3b",
                    model_source="meta-llama/Llama-3.2-3b-instruct",
                ),
                deployment_config=dict(
                    autoscaling_config=dict(
                        min_replicas=1,
                        max_replicas=2,
                    )
                ),
                accelerator_type="A10G",
            )

            # Build the application
            llm_app = build_openai_app([llm_config1, llm_config2])

            # Deploy the application
            serve.run(llm_app)

    Args:
        llm_serving_args: The list of llm configs or the paths to the llm config to
            build the app.

    Returns:
        The configured Ray Serve Application router.
    """
    from ray.llm._internal.serve.builders import build_openai_app

    return build_openai_app(llm_serving_args=llm_serving_args)
