"""Tests that the ingress returns errors in the OpenAI error format."""

import sys

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
    EmbeddingRequest,
)
from ray.llm._internal.serve.core.ingress.middleware import (
    SetRequestIdMiddleware,
    add_exception_handling_middleware,
)

REQUEST_ID = "req-1"
MESSAGES = [{"role": "user", "content": "hi"}]


@pytest.fixture
def client():
    app = FastAPI()

    @app.post("/chat")
    def chat(body: ChatCompletionRequest):
        return {}

    @app.post("/completions")
    def completions(body: CompletionRequest):
        return {}

    @app.post("/embeddings")
    def embeddings(body: EmbeddingRequest):
        return {}

    @app.get("/raise/{status_code}")
    def raise_http_exception(status_code: int):
        raise HTTPException(
            status_code=status_code,
            detail="Could not find model.",
            headers={"Retry-After": "1"},
        )

    add_exception_handling_middleware(app)
    app.add_middleware(SetRequestIdMiddleware)
    return TestClient(
        app, raise_server_exceptions=False, headers={"x-request-id": REQUEST_ID}
    )


def test_http_exception_uses_error_envelope(client):
    response = client.get("/raise/404")

    assert response.status_code == 404
    assert response.headers["retry-after"] == "1"
    assert response.json() == {
        "error": {
            "message": f"Could not find model. (Request ID: {REQUEST_ID})",
            "type": "HTTPException",
            "param": None,
            "code": 404,
        }
    }


def test_http_exception_5xx_message_is_generic(client):
    response = client.get("/raise/500")

    assert response.status_code == 500
    error = response.json()["error"]
    assert error["message"] == f"Internal Server Error (Request ID: {REQUEST_ID})"
    assert error["type"] == "InternalServerError"


@pytest.mark.parametrize(
    ("method", "path", "status_code"),
    [("GET", "/unknown-route", 404), ("GET", "/chat", 405)],
)
def test_starlette_http_exception_uses_error_envelope(
    client, method, path, status_code
):
    response = client.request(method, path)

    assert response.status_code == status_code
    assert response.json()["error"]["code"] == status_code
    if status_code == 405:
        assert response.headers["allow"] == "POST"


@pytest.mark.parametrize(
    ("path", "body", "param"),
    [
        ("/chat", {"messages": {"role": "user", "content": "hi"}}, "messages"),
        ("/chat", {}, "messages"),
        ("/chat", {"messages": MESSAGES, "temperature": "hot"}, "temperature"),
        # Union and validator segments are not part of the path.
        ("/chat", {"messages": [{"role": "user", "content": 1}]}, "messages.0.content"),
        ("/chat", {"messages": [{"content": "hi"}]}, "messages.0.role"),
        ("/chat", {"messages": MESSAGES, "stop": 1}, "stop"),
        (
            "/chat",
            {"messages": MESSAGES, "tools": [{"type": "function"}]},
            "tools.0.function",
        ),
        (
            "/chat",
            {"messages": MESSAGES, "response_format": {"type": "x"}},
            "response_format.type",
        ),
        ("/completions", {"prompt": {"a": 1}}, "prompt"),
        ("/embeddings", {"input": {"a": 1}}, "input"),
    ],
)
def test_validation_error_param_is_the_field_path(client, path, body, param):
    response = client.post(path, json={"model": "m", **body})

    assert response.status_code == 400
    error = response.json()["error"]
    assert error["param"] == param
    assert error["type"] == "invalid_request_error"
    assert error["code"] == "invalid_parameter"


@pytest.mark.parametrize("content", [b"not json", b"{", b"", b"[1, 2]"])
def test_validation_error_without_a_field_has_no_param(client, content):
    response = client.post(
        "/chat", content=content, headers={"content-type": "application/json"}
    )

    assert response.status_code == 400
    assert response.json()["error"]["param"] is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
