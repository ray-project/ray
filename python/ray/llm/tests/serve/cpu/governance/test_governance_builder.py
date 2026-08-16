from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse as InternalBlockedResponse,
    RequestContext as InternalRequestContext,
)
from ray.llm._internal.serve.core.governance.ingress import (
    GovernanceIngress as InternalGovernanceIngress,
)
from ray.llm._internal.serve.core.governance.middleware import (
    LLMMiddleware as InternalLLMMiddleware,
)
from ray.llm._internal.serve.core.ingress.builder import build_openai_app
from ray.serve.llm.governance import (
    BlockedResponse,
    GovernanceIngress,
    LLMMiddleware,
    RequestContext,
)


class DummyMiddleware(LLMMiddleware):
    async def before_inference(self, request, context):
        return request


def test_build_openai_app_passes_middlewares_to_governance_ingress(
    mock_llm_config, disable_placement_bundles
):
    middleware = DummyMiddleware()
    app = build_openai_app(
        {
            "llm_configs": [mock_llm_config],
            "ingress_cls_config": {
                "ingress_cls": GovernanceIngress,
                "ingress_extra_kwargs": {"middlewares": [middleware]},
            },
        }
    )

    init_kwargs = app._bound_deployment.init_kwargs
    assert init_kwargs["middlewares"] == [middleware]
    assert app._bound_deployment.func_or_class.__name__ == "GovernanceIngress"


def test_public_governance_types_are_subclasses_of_internal():
    assert issubclass(BlockedResponse, InternalBlockedResponse)
    assert issubclass(RequestContext, InternalRequestContext)
    assert issubclass(LLMMiddleware, InternalLLMMiddleware)
    assert issubclass(GovernanceIngress, InternalGovernanceIngress)
    blocked = BlockedResponse(rule_triggered="ACCESS_DENIED", reason="no")
    assert isinstance(blocked, InternalBlockedResponse)
