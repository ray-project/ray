import sys
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from starlette.datastructures import Headers

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ErrorInfo,
    ErrorResponse,
    TokenizeChatRequest,
    TokenizeCompletionRequest,
)
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_openai_app,
)
from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.core.ingress.tokenizer import TokenizeError, Tokenizer
from ray.serve.experimental.round_robin_router import RoundRobinRouter
from ray.serve.llm.request_router import KVAwareRouter


class _TokenizeResponse:
    def __init__(self, tokens):
        self.tokens = tokens


async def _tokenize_stream(response):
    yield response


def _handle_returning(response):
    """A DeploymentHandle whose /tokenize streams ``response``; captures the
    Tokenize* request it was called with under ``captured``."""
    captured = {}

    def tokenize_remote(tok_req, _):
        captured["request"] = tok_req
        return _tokenize_stream(response)

    handle = MagicMock()
    handle.options.return_value.tokenize.remote = tokenize_remote
    return handle, captured


class TestTokenizer:
    @pytest.mark.parametrize(
        "payload",
        [
            {"model": "m", "prompt": ["a", "b"]},  # batch of prompts
            {"model": "m", "prompt": [1, 2, 3]},  # pre-tokenized token ids
            {"model": "m"},  # neither messages nor prompt
        ],
    )
    @pytest.mark.asyncio
    async def test_untokenizable_payload_returns_none(self, payload):
        """A parsed payload with no single-string prompt yields None."""
        assert await Tokenizer(MagicMock()).tokenize(payload) is None

    @pytest.mark.parametrize(
        "payload, expected_request_type",
        [
            (
                {"model": "m", "messages": [{"role": "user", "content": "hi"}]},
                TokenizeChatRequest,
            ),
            ({"model": "m", "prompt": "hello"}, TokenizeCompletionRequest),
        ],
    )
    @pytest.mark.asyncio
    async def test_tokenizes_chat_and_completion(self, payload, expected_request_type):
        """A chat or completion payload is sent to /tokenize as the right
        Tokenize* request and its returned token ids are surfaced."""
        handle, captured = _handle_returning(_TokenizeResponse([5, 6, 7]))
        tokens = await Tokenizer(handle).tokenize(payload)
        assert tokens == [5, 6, 7]
        assert isinstance(captured["request"], expected_request_type)

    @pytest.mark.parametrize(
        "payload, expected",
        [
            (  # chat: template-rendering fields + request-provided prompt flags
                {
                    "model": "m",
                    "messages": [{"role": "user", "content": "hi"}],
                    "tools": [
                        {
                            "type": "function",
                            "function": {"name": "f", "parameters": {}},
                        }
                    ],
                    "chat_template": "TEMPLATE",
                    "chat_template_kwargs": {"enable_thinking": False},
                    "mm_processor_kwargs": {"num_crops": 4},
                    "add_generation_prompt": False,
                    "continue_final_message": True,
                    "temperature": 0.7,
                },
                {
                    "chat_template": "TEMPLATE",
                    "chat_template_kwargs": {"enable_thinking": False},
                    "mm_processor_kwargs": {"num_crops": 4},
                    "add_generation_prompt": False,
                    "continue_final_message": True,
                },
            ),
            (  # completion: add_special_tokens comes from the request
                {
                    "model": "m",
                    "prompt": "hi",
                    "add_special_tokens": False,
                    "temperature": 0.7,
                },
                {"add_special_tokens": False},
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_forwards_prompt_fields_only(self, payload, expected):
        """Prompt-rendering fields come from the request (not hardcoded) and
        sampling params are dropped, so routing ids match prefill."""
        handle, captured = _handle_returning(_TokenizeResponse([1, 2]))
        await Tokenizer(handle).tokenize(payload)
        request = captured["request"]
        for attr, value in expected.items():
            assert getattr(request, attr) == value
        assert "temperature" not in (request.model_extra or {})

    @pytest.mark.asyncio
    async def test_error_response_raises(self):
        """A /tokenize ErrorResponse surfaces as a TokenizeError carrying vLLM's
        status code, message, and type."""
        err = ErrorResponse(
            error=ErrorInfo(message="bad model", type="NotFoundError", code=404)
        )
        handle, _ = _handle_returning(err)
        with pytest.raises(TokenizeError) as exc_info:
            await Tokenizer(handle).tokenize({"model": "m", "prompt": "hi"})
        assert exc_info.value.status_code == 404
        assert exc_info.value.message == "bad model"
        assert exc_info.value.type == "NotFoundError"

    @pytest.mark.asyncio
    async def test_empty_response_raises(self):
        """An empty /tokenize stream raises rather than returning no tokens."""

        async def _empty(*_args):
            for _ in ():
                yield

        handle = MagicMock()
        handle.options.return_value.tokenize.remote = _empty
        with pytest.raises(TokenizeError) as exc_info:
            await Tokenizer(handle).tokenize({"model": "m", "prompt": "hi"})
        assert exc_info.value.status_code == 500


class TestRoute:
    @pytest.mark.asyncio
    async def test_no_tokenizer_forwards_none(self):
        # A non-KV router has no tokenizer, so route forwards request_token_ids=None.
        router = LLMRouter.__new__(LLMRouter)
        router._handle = MagicMock()
        router._tokenizer = None
        router._pick_replica = AsyncMock(return_value=("h", 1, "rid"))

        request = MagicMock()
        request.body = AsyncMock(return_value=b'{"model": "m", "prompt": "hi"}')
        request.headers = Headers({})
        await router.route(request)
        assert router._pick_replica.call_args.kwargs["request_token_ids"] is None

    @pytest.mark.asyncio
    async def test_forwards_token_ids(self):
        # A successful tokenization forwards its token ids to _pick_replica.
        router = LLMRouter.__new__(LLMRouter)
        router._handle = MagicMock()
        router._tokenizer = MagicMock()
        router._tokenizer.tokenize = AsyncMock(return_value=[5, 6, 7])
        router._pick_replica = AsyncMock(return_value=("h", 1, "rid"))

        request = MagicMock()
        request.body = AsyncMock(return_value=b'{"model": "m", "prompt": "hi"}')
        request.headers = Headers({})
        await router.route(request)
        assert router._pick_replica.call_args.kwargs["request_token_ids"] == [5, 6, 7]

    @pytest.mark.asyncio
    async def test_unparseable_body_skips_tokenization(self):
        # A truncated/unparseable body derives no routing payload, so the
        # tokenizer is never called and request_token_ids stays None.
        router = LLMRouter.__new__(LLMRouter)
        router._handle = MagicMock()
        router._tokenizer = MagicMock()
        router._tokenizer.tokenize = AsyncMock(return_value=[5, 6, 7])
        router._pick_replica = AsyncMock(return_value=("h", 1, "rid"))

        request = MagicMock()
        # Truncated prefix: not valid JSON, so it can't be parsed or tokenized.
        request.body = AsyncMock(return_value=b'{"model": "m", "prompt": "' + b"x" * 8)
        request.headers = Headers({"x-body-truncated": "8/90000"})
        await router.route(request)

        router._tokenizer.tokenize.assert_not_called()
        assert router._pick_replica.call_args.kwargs["request_token_ids"] is None

    @pytest.mark.asyncio
    async def test_tokenize_error_becomes_http_error(self):
        # A /tokenize rejection becomes an HTTPException with the same status
        # code, and routing is not attempted.
        router = LLMRouter.__new__(LLMRouter)
        router._handle = MagicMock()
        router._tokenizer = MagicMock()
        router._tokenizer.tokenize = AsyncMock(
            side_effect=TokenizeError(
                "bad model", status_code=404, type="NotFoundError"
            )
        )
        router._pick_replica = AsyncMock()

        request = MagicMock()
        request.body = AsyncMock(return_value=b'{"model": "m", "prompt": "hi"}')
        request.headers = Headers({})
        with pytest.raises(HTTPException) as exc_info:
            await router.route(request)
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "bad model"
        router._pick_replica.assert_not_called()


def _build_llm_app(request_router_class):
    """Build a direct-streaming OpenAI app, optionally pinning a router class."""
    deployment_config = {"autoscaling_config": {"min_replicas": 1, "max_replicas": 1}}
    if request_router_class is not None:
        deployment_config["request_router_config"] = {
            "request_router_class": request_router_class
        }
    llm_config = LLMConfig(
        model_loading_config={
            "model_id": "qwen3-0.6b",
            "model_source": "Qwen/Qwen3-0.6B",
        },
        accelerator_type=None,
        deployment_config=deployment_config,
    )
    return build_openai_app(LLMServingArgs(llm_configs=[llm_config]))


def _pre_routing_tokenization(app) -> Optional[bool]:
    init_kwargs = app._ingress_request_router._bound_deployment.init_kwargs
    return init_kwargs["pre_routing_tokenization"]


class TestPreRoutingTokenization:
    """build_openai_app enables pre-routing tokenization iff the router is KV-aware."""

    @pytest.fixture(autouse=True)
    def enable_direct_streaming(self, monkeypatch):
        monkeypatch.setattr(
            "ray.llm._internal.serve.core.ingress.builder."
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
            True,
        )

    @pytest.mark.parametrize(
        "request_router_class, expected",
        [
            (KVAwareRouter, True),
            (None, False),
            (RoundRobinRouter, False),
        ],
    )
    def test_enabled_only_for_kv_aware_router(self, request_router_class, expected):
        app = _build_llm_app(request_router_class)
        assert _pre_routing_tokenization(app) is expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
