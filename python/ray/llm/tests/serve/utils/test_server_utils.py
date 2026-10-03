"""Tests for exception -> HTTP response mapping in server_utils."""

import sys

import pytest
from fastapi import HTTPException

from ray.llm._internal.serve.core.configs.openai_api_models import OpenAIHTTPException
from ray.llm._internal.serve.utils.server_utils import get_response_for_error

REQUEST_ID = "req-1"


def test_server_errors_do_not_leak_the_exception():
    """A 5xx body must not carry the original exception (it is logged instead).

    An error raised in a replica reaches the ingress as a RayTaskError whose
    str() is the whole remote traceback.
    """
    exc = RuntimeError(
        "ray::ServeReplica:llm:LLMServer.chat() (pid=7081, ip=10.0.0.1)\n"
        '  File "/opt/venv/lib/python3.12/site-packages/vllm/engine.py", line 42\n'
        "    raise AssertionError(SECRET_INTERNAL_STATE)"
    )

    error = get_response_for_error(exc, REQUEST_ID).error

    assert error.code == 500
    assert error.type == "InternalServerError"
    assert error.message == f"Internal Server Error (Request ID: {REQUEST_ID})"
    assert "SECRET_INTERNAL_STATE" not in error.message
    assert "pid=" not in error.message
    assert "site-packages" not in error.message


def test_client_errors_keep_their_message():
    """A 4xx must still tell the caller what was wrong."""
    exc = OpenAIHTTPException(status_code=400, message="prompt is too long")

    error = get_response_for_error(exc, REQUEST_ID).error

    assert error.code == 400
    assert error.message == f"prompt is too long (Request ID: {REQUEST_ID})"


def test_client_error_message_is_not_repeated():
    """The message used to be emitted three times in one string."""
    error = get_response_for_error(
        HTTPException(status_code=400, detail="bad input"), REQUEST_ID
    ).error

    assert error.message.count("bad input") == 1


def test_internal_message_is_not_sent_to_the_client():
    """OpenAIHTTPException.internal_message is internal; it must stay out of the body."""
    exc = OpenAIHTTPException(
        status_code=408,
        message="Request server side timeout",
        internal_message="TimeoutError at replica 0x7f34c5f5dac0",
    )

    error = get_response_for_error(exc, REQUEST_ID).error

    assert error.code == 408
    assert "0x7f34c5f5dac0" not in error.message


@pytest.mark.parametrize(
    ("exc", "expected_code"),
    [
        (HTTPException(status_code=404, detail="not found"), 404),
        (OpenAIHTTPException(status_code=429, message="slow down"), 429),
        (RuntimeError("engine died"), 500),
        (ValueError("some value error"), 500),
    ],
)
def test_status_mapping_is_unchanged(exc, expected_code):
    assert get_response_for_error(exc, REQUEST_ID).error.code == expected_code


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
