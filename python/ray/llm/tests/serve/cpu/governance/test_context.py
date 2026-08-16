from typing import Any, Dict, Optional

import pytest
from starlette.requests import Request

from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse,
    blocked_response_to_http,
    build_request_context,
)


class _FakeBody:
    def __init__(
        self,
        model: str = "test-model",
        user: Optional[str] = None,
        max_tokens: Optional[int] = None,
        max_completion_tokens: Optional[int] = None,
    ):
        self.model = model
        self.user = user
        self.max_tokens = max_tokens
        self.max_completion_tokens = max_completion_tokens


def _make_request(
    headers: Optional[Dict[str, str]] = None,
    state: Optional[Dict[str, Any]] = None,
) -> Request:
    header_list = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    request = Request(
        {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/v1/chat/completions",
            "raw_path": b"/v1/chat/completions",
            "query_string": b"",
            "headers": header_list,
            "client": ("127.0.0.1", 123),
            "server": ("127.0.0.1", 80),
        }
    )
    for key, value in (state or {}).items():
        setattr(request.state, key, value)
    return request


def test_blocked_response_rejects_invalid_decision():
    with pytest.raises(ValueError, match="Invalid decision"):
        BlockedResponse(decision="NOPE")


def test_blocked_response_requires_retry_after_when_throttled():
    with pytest.raises(ValueError, match="retry_after"):
        BlockedResponse(decision="THROTTLED")


@pytest.mark.parametrize(
    "rule_triggered, expected_status",
    [
        ("PII_DETECTED", 400),
        ("BUDGET_EXCEEDED", 402),
        ("ACCESS_DENIED", 403),
        ("UNKNOWN_RULE", 403),
    ],
)
def test_blocked_response_http_status_mapping(rule_triggered, expected_status):
    response = blocked_response_to_http(BlockedResponse(rule_triggered=rule_triggered))
    assert response.status_code == expected_status


def test_throttled_response_sets_retry_after_header():
    response = blocked_response_to_http(
        BlockedResponse(
            decision="THROTTLED", rule_triggered="RATE_LIMIT", retry_after=30
        )
    )
    assert response.status_code == 429
    assert response.headers["retry-after"] == "30"


def test_build_request_context_without_http_request():
    context = build_request_context(
        _FakeBody(model="m1", user="body-user", max_tokens=16)
    )
    assert context.model_id == "m1"
    assert context.user_id == "body-user"
    assert context.max_tokens == 16
    assert context.request_id is None
    assert context.session_id is None
    assert context.tenant_id is None
    assert context.headers == {}
    assert context.estimated_input_tokens is None


def test_build_request_context_prefers_state_user_id():
    request = _make_request(
        headers={"x-user-id": "header-user"},
        state={"user_id": "state-user", "request_id": "req-state"},
    )
    context = build_request_context(_FakeBody(user="body-user"), request)
    assert context.user_id == "state-user"
    assert context.request_id == "req-state"


def test_build_request_context_falls_back_to_headers():
    request = _make_request(
        headers={
            "x-request-id": "req-header",
            "x-session-id": "sess-1",
            "x-user-id": "header-user",
            "x-tenant-id": "tenant-9",
        }
    )
    context = build_request_context(_FakeBody(), request)
    assert context.request_id == "req-header"
    assert context.session_id == "sess-1"
    assert context.user_id == "header-user"
    assert context.tenant_id == "tenant-9"
    assert context.headers["x-tenant-id"] == "tenant-9"


def test_build_request_context_reads_max_completion_tokens():
    context = build_request_context(_FakeBody(max_completion_tokens=32))
    assert context.max_tokens == 32
