from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse as _BlockedResponse,
    RequestContext as _RequestContext,
)
from ray.llm._internal.serve.core.governance.ingress import (
    GovernanceIngress as _GovernanceIngress,
)
from ray.llm._internal.serve.core.governance.middleware import (
    LLMMiddleware as _LLMMiddleware,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class RequestContext(_RequestContext):
    """Per-request metadata passed to governance middleware hooks.

    Attributes:
        model_id: Served model ID from the request body.
        request_id: Request ID from Serve request state or the
            ``x-request-id`` header.
        session_id: Session ID derived from request headers.
        max_tokens: ``max_tokens`` or ``max_completion_tokens`` from the body.
        user_id: User ID from request state, the body ``user`` field, or the
            ``x-user-id`` header.
        tenant_id: Tenant ID from the ``x-tenant-id`` header.
        estimated_input_tokens: Optional estimated input token count.
        headers: Copy of the inbound HTTP headers.
    """

    pass


@PublicAPI(stability="alpha")
class BlockedResponse(_BlockedResponse):
    """Return this from a middleware hook to stop the request.

    Attributes:
        decision: ``"BLOCKED"`` (default) or ``"THROTTLED"``. Throttled
            responses are returned as HTTP 429 and require ``retry_after``.
        rule_triggered: Identifier for the rule that fired, such as
            ``PII_DETECTED``, ``BUDGET_EXCEEDED``, or ``ACCESS_DENIED``.
        reason: Human-readable explanation returned in the error body.
        severity: ``"ERROR"`` (default) or ``"WARNING"``.
        retry_after: Seconds to wait before retrying. Required when
            ``decision`` is ``"THROTTLED"``.
    """

    pass


@PublicAPI(stability="alpha")
class LLMMiddleware(_LLMMiddleware):
    """Hook interface for request and response governance.

    Implement ``before_inference`` (required). Override ``after_inference``
    and ``on_inference_complete`` only when you need those hooks; the
    defaults pass the response through and do nothing after completion.

    Examples:
        .. testcode::
            :skipif: True

            from ray.serve.llm.governance import BlockedResponse, LLMMiddleware

            class DenyModelMiddleware(LLMMiddleware):
                async def before_inference(self, request, context):
                    if context.model_id == "blocked-model":
                        return BlockedResponse(
                            rule_triggered="ACCESS_DENIED",
                            reason="Model is not allowed",
                        )
                    return request
    """

    pass


@PublicAPI(stability="alpha")
class GovernanceIngress(_GovernanceIngress):
    """OpenAI-compatible ingress that runs governance middleware around inference.

    Use this in place of
    :class:`~ray.serve.llm.ingress.OpenAiIngress` when you want
    :class:`LLMMiddleware` hooks on the chat, completions, and transcriptions
    paths. Pass middleware instances with ``ingress_extra_kwargs``.

    Args:
        middlewares: Optional list of :class:`LLMMiddleware` instances, run
            in list order on each request.

    Examples:
        .. testcode::
            :skipif: True

            from ray.serve.llm import LLMConfig, build_openai_app
            from ray.serve.llm.governance import (
                BlockedResponse,
                GovernanceIngress,
                LLMMiddleware,
            )

            class DenyModelMiddleware(LLMMiddleware):
                async def before_inference(self, request, context):
                    if context.model_id == "blocked-model":
                        return BlockedResponse(
                            rule_triggered="ACCESS_DENIED",
                            reason="Model is not allowed",
                        )
                    return request

            llm_config = LLMConfig(
                model_loading_config=dict(
                    model_id="qwen-0.5b",
                    model_source="Qwen/Qwen2.5-0.5B-Instruct",
                ),
            )

            app = build_openai_app(
                {
                    "llm_configs": [llm_config],
                    "ingress_cls_config": {
                        "ingress_cls": GovernanceIngress,
                        "ingress_extra_kwargs": {
                            "middlewares": [DenyModelMiddleware()],
                        },
                    },
                }
            )
    """

    pass


__all__ = [
    "BlockedResponse",
    "GovernanceIngress",
    "LLMMiddleware",
    "RequestContext",
]
