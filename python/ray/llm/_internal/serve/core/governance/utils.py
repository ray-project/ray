"""Helpers for extracting text from OpenAI-compatible request/response objects."""

from typing import Any, Iterable, List


def _coerce_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(_coerce_to_text(item) for item in value)
    if isinstance(value, dict):
        if isinstance(value.get("text"), str):
            return value["text"]
        if "content" in value:
            return _coerce_to_text(value["content"])
    return str(value)


def _message_content(message: Any) -> str:
    if isinstance(message, dict):
        return _coerce_to_text(message.get("content"))
    return _coerce_to_text(getattr(message, "content", ""))


def extract_request_text(request: Any) -> str:
    """Extract user-visible text from a chat or completion request body."""
    parts: List[str] = []

    messages = getattr(request, "messages", None)
    if messages:
        for message in messages:
            parts.append(_message_content(message))

    prompt = getattr(request, "prompt", None)
    if prompt is not None:
        if isinstance(prompt, str):
            parts.append(prompt)
        elif isinstance(prompt, list):
            parts.extend(_coerce_to_text(item) for item in prompt)
        else:
            parts.append(_coerce_to_text(prompt))

    return "\n".join(part for part in parts if part)


def extract_response_text(response: Any) -> str:
    """Extract assistant text from a chat/completion response or stream chunk."""
    parts: List[str] = []
    if isinstance(response, dict):
        choices: Iterable[Any] = response.get("choices") or []
    else:
        choices = getattr(response, "choices", None) or []

    for choice in choices:
        if isinstance(choice, dict):
            message = choice.get("message") or choice.get("delta") or {}
            if isinstance(message, dict):
                parts.append(_coerce_to_text(message.get("content")))
            else:
                parts.append(_coerce_to_text(message))
        else:
            message = getattr(choice, "message", None) or getattr(choice, "delta", None)
            if message is not None:
                parts.append(_message_content(message))

    return "\n".join(part for part in parts if part)
