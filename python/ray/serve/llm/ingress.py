from ray.llm._internal.serve.core.ingress.ingress import (
    OpenAiIngress as _OpenAiIngress,
    make_fastapi_ingress,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="beta")
class OpenAiIngress(_OpenAiIngress):

    """The implementation of the OpenAI compatible model router.

    This deployment creates the following endpoints:
      - /v1/chat/completions: Chat interface (OpenAI-style)
      - /v1/completions: Text completion
      - /v1/models: List available models
      - /v1/models/{model}: Model information
      - /v1/embeddings: Text embeddings
      - /v1/audio/transcriptions: Audio transcription
      - /v1/score: Text scoring


    Examples:
        .. testcode::
            :skipif: True


            from ray import serve
            from ray.llm._internal.serve.core.configs.openai_api_models import (
                to_model_metadata,
            )
            from ray.serve.llm import LLMConfig
            from ray.serve.llm.deployment import LLMServer
            from ray.serve.llm.ingress import OpenAiIngress, make_fastapi_ingress

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

            # deployment #1
            server_options1 = LLMServer.get_deployment_options(llm_config1)
            server_deployment1 = serve.deployment(LLMServer).options(
                **server_options1).bind(llm_config1)

            # deployment #2
            server_options2 = LLMServer.get_deployment_options(llm_config2)
            server_deployment2 = serve.deployment(LLMServer).options(
                **server_options2).bind(llm_config2)

            # ingress: pass dicts keyed by model_id; no remote llm_config fetch.
            ingress_options = OpenAiIngress.get_deployment_options(
                llm_configs=[llm_config1, llm_config2])
            ingress_cls = make_fastapi_ingress(OpenAiIngress)
            ingress_deployment = (
                serve.deployment(ingress_cls)
                .options(**ingress_options)
                .bind(
                    llm_deployments={
                        llm_config1.model_id: server_deployment1,
                        llm_config2.model_id: server_deployment2,
                    },
                    model_cards={
                        llm_config1.model_id: to_model_metadata(
                            llm_config1.model_id, llm_config1
                        ),
                        llm_config2.model_id: to_model_metadata(
                            llm_config2.model_id, llm_config2
                        ),
                    },
                )
            )

            # run
            serve.run(ingress_deployment, blocking=True)
    """

    pass


__all__ = ["OpenAiIngress", "make_fastapi_ingress"]
