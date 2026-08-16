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
    request_id: Optional[str] = None
    session_id: Optional[str] = None
    max_tokens: Optional[int] = None
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    estimated_input_tokens: Optional[int] = None
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class BlockedResponse:
    decision: str = "BLOCKED"
    rule_triggered: str = ""
    reason: str = ""
    severity: str = "ERROR"
    retry_after: Optional[int] = None

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
        return usage.model_dump()
    if isinstance(usage, dict):
        return usage
    return {}
