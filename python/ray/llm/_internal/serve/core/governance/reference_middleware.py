"""Reference governance middleware for testing and documentation.

These are intentionally simple, in-memory implementations. Production adapters
(TealTiger, AgentShield, etc.) should subclass ``LLMMiddleware`` separately.
"""

import re
from typing import Any, Dict, Optional, Union

from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse,
    RequestContext,
)
from ray.llm._internal.serve.core.governance.middleware import LLMMiddleware
from ray.llm._internal.serve.core.governance.utils import (
    extract_request_text,
    extract_response_text,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)

EMAIL_PATTERN = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
)
SSN_PATTERN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")


class PIIMiddleware(LLMMiddleware):
    """Block requests/responses that contain common PII patterns (email, SSN)."""

    def __init__(
        self,
        *,
        scan_requests: bool = True,
        scan_responses: bool = True,
    ) -> None:
        self._scan_requests = scan_requests
        self._scan_responses = scan_responses

    def _detect_pii(self, text: str) -> Optional[str]:
        if EMAIL_PATTERN.search(text):
            return "email address"
        if SSN_PATTERN.search(text):
            return "SSN"
        return None

    async def before_inference(
        self,
        request: Any,
        context: RequestContext,
    ) -> Union[Any, BlockedResponse]:
        if not self._scan_requests:
            return request

        detected = self._detect_pii(extract_request_text(request))
        if detected is not None:
            logger.info(
                "PII detected in request model=%s request_id=%s type=%s",
                context.model_id,
                context.request_id,
                detected,
            )
            return BlockedResponse(
                rule_triggered="PII_DETECTED",
                reason=f"Request contains {detected}",
            )
        return request

    async def after_inference(
        self,
        request: Any,
        response: Any,
        context: RequestContext,
    ) -> Any:
        if not self._scan_responses:
            return response

        detected = self._detect_pii(extract_response_text(response))
        if detected is not None:
            logger.info(
                "PII detected in response model=%s request_id=%s type=%s",
                context.model_id,
                context.request_id,
                detected,
            )
            return BlockedResponse(
                rule_triggered="PII_DETECTED",
                reason=f"Response contains {detected}",
            )
        return response


class BudgetMiddleware(LLMMiddleware):
    """Enforce a per-user cumulative token budget using in-memory counters.

    Counters live on this instance, so they are per ingress replica. This is a
    reference implementation for tests and examples, not a production budget
    store — adapters that need a cluster-wide budget should use shared state.
    """

    def __init__(
        self,
        *,
        token_budget: int = 10_000,
        default_user_id: str = "anonymous",
    ) -> None:
        if token_budget <= 0:
            raise ValueError("token_budget must be positive")
        self._token_budget = token_budget
        self._default_user_id = default_user_id
        self._usage_by_user: Dict[str, int] = {}

    def _user_key(self, context: RequestContext) -> str:
        return context.user_id or self._default_user_id

    def _current_usage(self, context: RequestContext) -> int:
        return self._usage_by_user.get(self._user_key(context), 0)

    async def before_inference(
        self,
        request: Any,
        context: RequestContext,
    ) -> Union[Any, BlockedResponse]:
        user_key = self._user_key(context)
        current_usage = self._current_usage(context)
        if current_usage >= self._token_budget:
            logger.info(
                "Budget exceeded for user=%s model=%s usage=%s budget=%s",
                user_key,
                context.model_id,
                current_usage,
                self._token_budget,
            )
            return BlockedResponse(
                rule_triggered="BUDGET_EXCEEDED",
                reason=(
                    f"Token budget exceeded for user {user_key}: "
                    f"{current_usage}/{self._token_budget}"
                ),
            )
        return request

    async def on_inference_complete(
        self,
        usage: Dict[str, Any],
        context: RequestContext,
    ) -> None:
        total_tokens = int(usage.get("total_tokens") or 0)
        if total_tokens <= 0:
            return

        user_key = self._user_key(context)
        updated_usage = self._current_usage(context) + total_tokens
        self._usage_by_user[user_key] = updated_usage
        logger.debug(
            "Updated token budget usage user=%s model=%s usage=%s/%s",
            user_key,
            context.model_id,
            updated_usage,
            self._token_budget,
        )
