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
            model_handle = serve.run(vllm_app)

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
            from ray.serve.llm.deployments import VLLMService, LLMRouter

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
            deployment1 = VLLMService.as_deployment().bind(llm_config1)
            deployment2 = VLLMService.as_deployment().bind(llm_config2)
            llm_app = LLMRouter.as_deployment().bind([deployment1, deployment2])
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

    Args:
        llm_serving_args: The list of llm configs or the paths to the llm config to
            build the app.

    Returns:
        The configured Ray Serve Application router.
    """
    from ray.llm._internal.serve.builders import build_openai_app

    return build_openai_app(llm_serving_args=llm_serving_args)
