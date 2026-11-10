"""The requests the KV-aware P/D router runs prefill for, and prefill's result."""

from typing import Any, Dict, Optional, Union

from pydantic import ValidationError

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
    ErrorResponse,
)
from ray.llm._internal.serve.core.ingress.utils import _sanitize_chat_completion_request

PDRequest = Union[ChatCompletionRequest, CompletionRequest]
# KV transfer params for D, or P's error for the client.
PrefillResult = Union[Dict[str, Any], ErrorResponse]

# P/D covers only chat and completions: PDDecodeServer and the KV connector
# backend handle only these request types. Other routes go to D, which serves
# them without P/D.
_PD_REQUEST_TYPES = {
    "/v1/chat/completions": ChatCompletionRequest,
    "/v1/completions": CompletionRequest,
}


def parse_pd_request(path: str, body: Optional[Dict[str, Any]]) -> Optional[PDRequest]:
    """The typed request P runs, or None when D serves the request alone."""
    request_cls = _PD_REQUEST_TYPES.get(path.split("?", 1)[0])
    if request_cls is None or body is None:
        return None
    try:
        request = request_cls.model_validate(body)
    except ValidationError:
        return None
    if isinstance(request, ChatCompletionRequest):
        request = _sanitize_chat_completion_request(request)
    return request
