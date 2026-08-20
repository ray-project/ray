"""Governance middleware for Ray Serve LLM.

Provides hook points for PII detection, budget enforcement, access control,
and audit trails via ``GovernanceIngress`` and ``LLMMiddleware`` implementations.
"""

from typing import TYPE_CHECKING

from ray.llm._internal.serve.core.governance.chain import MiddlewareChain
from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse,
    RequestContext,
    blocked_response_to_http,
    build_request_context,
    usage_to_dict,
)
from ray.llm._internal.serve.core.governance.middleware import LLMMiddleware
from ray.llm._internal.serve.core.governance.reference_middleware import (
    BudgetMiddleware,
    PIIMiddleware,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.governance.ingress import GovernanceIngress

# ``GovernanceIngress`` imports OpenAI protocol models, which require vLLM or
# SGLang. Load it on first access so chain/context/middleware unit tests can
# import this package without an inference engine.
_LAZY_ATTRS = {
    "GovernanceIngress": ("ingress", "GovernanceIngress"),
}

__all__ = [
    "BlockedResponse",
    "BudgetMiddleware",
    "GovernanceIngress",
    "LLMMiddleware",
    "MiddlewareChain",
    "PIIMiddleware",
    "RequestContext",
    "blocked_response_to_http",
    "build_request_context",
    "usage_to_dict",
]


def __getattr__(name):
    try:
        submodule, attr = _LAZY_ATTRS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None

    import importlib

    module = importlib.import_module(f"{__name__}.{submodule}")
    value = getattr(module, attr)
    globals()[name] = value
    return value


def __dir__():
    return sorted({*globals().keys(), *_LAZY_ATTRS.keys()})
