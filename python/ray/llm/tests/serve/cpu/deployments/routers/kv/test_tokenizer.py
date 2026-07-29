import sys
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from starlette.datastructures import Headers

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    TokenizeCompletionRequest,
)
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_openai_app,
)
from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.tokenizer import (
    TokenizeError,
    build_tokenize_request,
)
from ray.serve.experimental.round_robin_router import RoundRobinRouter
from ray.serve.llm.request_router import KVAwareRouter


class TestBuildTokenizeRequest:
    @pytest.mark.parametrize(
        "payload",
        [
            {"model": "m", "prompt": ["a", "b"]},  # batch of prompts
            {"model": "m", "prompt": [1, 2, 3]},  # pre-tokenized token ids
            {"model": "m"},  # neither messages nor prompt
        ],
    )
    def test_untokenizable_payload_returns_none(self, payload):
        """A parsed payload with no single-string prompt yields None, so the
        caller falls back to token-less routing."""
        assert build_tokenize_request(payload) is None

    @pytest.mark.parametrize(
        "payload, expected_request_type",
        [
            (
                {"model": "m", "messages": [{"role": "user", "content": "hi"}]},
                ChatCompletionRequest,
            ),
            ({"model": "m", "prompt": "hello"}, TokenizeCompletionRequest),
        ],
    )
    def test_builds_chat_and_completion_requests(self, payload, expected_request_type):
        """A chat or completion payload builds the right Tokenize* request."""
        assert isinstance(build_tokenize_request(payload), expected_request_type)

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
    def test_forwards_prompt_fields_only(self, payload, expected):
        """Prompt-rendering fields come from the request (not hardcoded) and
        sampling params are dropped, so routing ids match prefill."""
        request = build_tokenize_request(payload)
        for attr, value in expected.items():
            assert getattr(request, attr) == value
        assert "temperature" not in (request.model_extra or {})


class TestRoute:
    @pytest.mark.asyncio
    async def test_no_tokenizer_forwards_no_token_ids(self):
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


def _build_llm_app(request_router_class, runtime_env=None):
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
        runtime_env=runtime_env,
    )
    return build_openai_app(LLMServingArgs(llm_configs=[llm_config]))


def _router_init_kwargs(app) -> Dict[str, Any]:
    return app._ingress_request_router._bound_deployment.init_kwargs


def _router_ray_actor_options(app) -> Dict[str, Any]:
    return app._ingress_request_router._bound_deployment.ray_actor_options


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
        init_kwargs = _router_init_kwargs(app)
        # A non-None llm_config is the sole signal for pre-routing tokenization;
        # it must be bound exactly when the router is KV-aware.
        assert (init_kwargs["llm_config"] is not None) is expected

    def test_runtime_env_reaches_tokenizing_router(self):
        app = _build_llm_app(
            KVAwareRouter,
            runtime_env={"env_vars": {"VLLM_USE_FASTOKENS": "1", "EXTRA_VAR": "value"}},
        )
        runtime_env = _router_ray_actor_options(app)["runtime_env"]
        llm_config = _router_init_kwargs(app)["llm_config"]
        assert runtime_env == {"env_vars": llm_config.runtime_env["env_vars"]}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
