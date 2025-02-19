from typing import TYPE_CHECKING

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.serve.deployment import Application
    from ray.serve.llm.configs import LLMConfig, LLMServingArgs


@PublicAPI(stability="alpha")
def build_vllm_deployment(llm_config: "LLMConfig") -> "Application":
    """Helper to build a single vllm deployment from the given llm config.

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


    Args:
        llm_serving_args: The list of llm configs or the paths to the llm config to
            build the app.

    Returns:
        The configured Ray Serve Application router.
    """
    from ray.llm._internal.serve.builders import build_openai_app

    return build_openai_app(llm_serving_args=llm_serving_args)

    # Examples:
    #     .. testcode::
    #         :skipif: True

    #         # from ray import serve
    #         # from ray.serve.config import AutoscalingConfig
    #         # from ray.serve.llm.configs import LLMConfig, ModelLoadingConfig, DeploymentConfig
    #         # from ray.serve.llm.builders import build_vllm_deployment

    #         # # Configure the model
    #         # llm_config = LLMConfig(
    #         #     model_loading_config=ModelLoadingConfig(
    #         #         served_model_name="llama-3.1-8b",
    #         #         model_source="meta-llama/Llama-3.1-8b-instruct",
    #         #     ),
    #         #     deployment_config=DeploymentConfig(
    #         #         autoscaling_config=AutoscalingConfig(
    #         #             min_replicas=1,
    #         #             max_replicas=8,
    #         #         )
    #         #     ),
    #         # )

    #         # # Build the deployment
    #         # llm_app = build_vllm_deployment(llm_config)

    #         # # Deploy the application
    #         # serve.run(llm_app)

    # Examples:
    #     .. testcode::
    #         :skipif: True

    #         assert False
    #         # from ray import serve
    #         # from ray.serve.config import AutoscalingConfig
    #         # from ray.serve.llm.configs import LLMConfig, ModelLoadingConfig, DeploymentConfig
    #         # from ray.serve.llm.builders import build_openai_app

    #         # # Configure multiple models
    #         # llm_config1 = LLMConfig(
    #         #     model_loading_config=ModelLoadingConfig(
    #         #         served_model_name="llama-3.1-8b",
    #         #         model_source="meta-llama/Llama-3.1-8b-instruct",
    #         #     ),
    #         #     deployment_config=DeploymentConfig(
    #         #         autoscaling_config=AutoscalingConfig(
    #         #             min_replicas=1,
    #         #             max_replicas=8,
    #         #         )
    #         #     ),
    #         # )

    #         # llm_config2 = LLMConfig(
    #         #     model_loading_config=ModelLoadingConfig(
    #         #         served_model_name="llama-3.2-3b",
    #         #         model_source="meta-llama/Llama-3.2-3b-instruct",
    #         #     ),
    #         #     deployment_config=DeploymentConfig(
    #         #         autoscaling_config=AutoscalingConfig(
    #         #             min_replicas=1,
    #         #             max_replicas=8,
    #         #         )
    #         #     ),
    #         # )

    #         # # Build the application
    #         # llm_app = build_openai_app([llm_config1, llm_config2])

    #         # # Deploy the application
    #         # serve.run(llm_app)
