from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from starlette.requests import Request
from starlette.responses import JSONResponse

from ray.serve._private.http_util import session_id_from_headers

_RULE_TO_STATUS_CODE = {
    "PII_DETECTED": 400,
    "BUDGET_EXCEEDED": 402,
    "ACCESS_DENIED": 403,
}


@dataclass
class RequestContext:
    model_id: str
    """Served model ID from the request body."""
    request_id: Optional[str] = None
    """Request ID from Serve request state or the ``x-request-id`` header."""
    session_id: Optional[str] = None
    """Session ID derived from request headers."""
    max_tokens: Optional[int] = None
    """``max_tokens`` or ``max_completion_tokens`` from the body."""
    user_id: Optional[str] = None
    """User ID from request state, the body ``user`` field, or the ``x-user-id`` header."""
    tenant_id: Optional[str] = None
    """Tenant ID from the ``x-tenant-id`` header."""
    estimated_input_tokens: Optional[int] = None
    """Optional estimated input token count."""
    headers: Dict[str, str] = field(default_factory=dict)
    """Copy of the inbound HTTP headers."""


@dataclass
class BlockedResponse:
    decision: str = "BLOCKED"
    """``BLOCKED`` (default) or ``THROTTLED``. Throttled responses are HTTP 429."""
    rule_triggered: str = ""
    """Rule that fired, such as ``PII_DETECTED``, ``BUDGET_EXCEEDED``, or ``ACCESS_DENIED``."""
    reason: str = ""
    """Human-readable explanation returned in the error body."""
    severity: str = "ERROR"
    """``ERROR`` (default) or ``WARNING``."""
    retry_after: Optional[int] = None
    """Seconds to wait before retrying. Required when ``decision`` is ``THROTTLED``."""

    def __post_init__(self) -> None:
        if self.decision not in ("BLOCKED", "THROTTLED"):
            raise ValueError(f"Invalid decision: {self.decision!r}")
        if self.severity not in ("ERROR", "WARNING"):
            raise ValueError(f"Invalid severity: {self.severity!r}")
        if self.decision == "THROTTLED" and self.retry_after is None:
            raise ValueError("THROTTLED blocks require retry_after")


def build_request_context(
    body: Any, raw_request: Optional[Request] = None
) -> RequestContext:
    """Build governance metadata from an OpenAI request body and HTTP request."""
    headers: Dict[str, str] = {}
    request_id: Optional[str] = None
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None

    if raw_request is not None:
        headers = dict(raw_request.headers)
        request_id = getattr(raw_request.state, "request_id", None)
        session_id = session_id_from_headers(raw_request.headers)
        user_id = getattr(raw_request.state, "user_id", None)
        tenant_id = raw_request.headers.get("x-tenant-id")
        if request_id is None:
            request_id = raw_request.headers.get("x-request-id")

    body_user = getattr(body, "user", None)
    if user_id is None and body_user is not None:
        user_id = str(body_user)
    if user_id is None and raw_request is not None:
        header_user_id = raw_request.headers.get("x-user-id")
        if header_user_id is not None:
            user_id = header_user_id

    max_tokens: Optional[int] = None
    for attr in ("max_tokens", "max_completion_tokens"):
        value = getattr(body, attr, None)
        if value is not None:
            max_tokens = value
            break

    return RequestContext(
        model_id=body.model,
        request_id=request_id,
        session_id=session_id,
        max_tokens=max_tokens,
        user_id=user_id,
        tenant_id=tenant_id,
        headers=headers,
    )


def blocked_response_to_http(blocked: BlockedResponse) -> JSONResponse:
    """Map a BlockedResponse to an OpenAI-style JSON HTTP response."""
    if blocked.decision == "THROTTLED":
        status_code = 429
    else:
        status_code = _RULE_TO_STATUS_CODE.get(blocked.rule_triggered, 403)

    content = {
        "error": {
            "message": blocked.reason,
            "type": blocked.rule_triggered or "governance_blocked",
            "code": blocked.rule_triggered or "governance_blocked",
        }
    }
    headers: Dict[str, str] = {}
    if blocked.retry_after is not None:
        headers["Retry-After"] = str(blocked.retry_after)

    return JSONResponse(status_code=status_code, content=content, headers=headers)


def usage_to_dict(response: Any) -> Dict[str, Any]:
    """Extract token usage metadata from a model response object."""
    usage = getattr(response, "usage", None)
    if usage is None:
        return {}
    if hasattr(usage, "model_dump"):
        dumped = usage.model_dump(exclude_none=True)
        return dumped if isinstance(dumped, dict) else {}
    if isinstance(usage, dict):
        return {key: value for key, value in usage.items() if value is not None}
    return {}
