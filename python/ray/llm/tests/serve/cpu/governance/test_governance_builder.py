from ray.llm._internal.serve.core.governance import GovernanceIngress, LLMMiddleware
from ray.llm._internal.serve.core.ingress.builder import build_openai_app


class DummyMiddleware(LLMMiddleware):
    async def before_inference(self, request, context):
        return request


def test_build_openai_app_passes_middlewares_to_governance_ingress(
    llm_config, disable_placement_bundles
):
    middleware = DummyMiddleware()
    app = build_openai_app(
        {
            "llm_configs": [llm_config],
            "ingress_cls_config": {
                "ingress_cls": GovernanceIngress,
                "ingress_extra_kwargs": {"middlewares": [middleware]},
            },
        }
    )

    init_kwargs = app._bound_deployment.init_kwargs
    assert init_kwargs["middlewares"] == [middleware]
    assert app._bound_deployment.func_or_class.__name__ == "GovernanceIngress"
