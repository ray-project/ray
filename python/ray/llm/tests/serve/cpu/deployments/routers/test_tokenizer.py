import sys
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.datastructures import Headers

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_openai_app,
)
from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.core.ingress.tokenizer import Tokenizer
from ray.serve.experimental.round_robin_router import RoundRobinRouter
from ray.serve.llm.request_router import KVAwareRouter


class TestTokenizer:
    @pytest.mark.parametrize(
        "body, body_truncated",
        [
            (b'{"model": "m", "prompt": "hi"}', True),  # truncated prefix
            (b"", False),  # empty
            (b'{"model": "m", "prompt": ["a", "b"]}', False),  # multi-prompt batch
            (b"not json {", False),  # invalid JSON
            (b"[1, 2, 3]", False),  # not a JSON object
            (b'{"model": "m"}', False),  # neither messages nor prompt
        ],
    )
    @pytest.mark.asyncio
    async def test_untokenizable(self, body, body_truncated):
        assert await Tokenizer(MagicMock()).tokenize(body, body_truncated) is None


class _MockTokenizer:
    async def tokenize(self, request_body, body_truncated):
        return None


class TestRoute:
    @pytest.mark.parametrize("tokenizer", [_MockTokenizer(), None])
    @pytest.mark.asyncio
    async def test_routes_without_token_ids(self, tokenizer):
        # Untokenizable request (tokenizer -> None) or non-KV router (no
        # tokenizer): route forwards request_token_ids=None.
        router = LLMRouter.__new__(LLMRouter)
        router._handle = MagicMock()
        router._tokenizer = tokenizer
        router._pick_replica = AsyncMock(return_value=("h", 1, "rid"))

        request = MagicMock()
        request.body = AsyncMock(return_value=b'{"model": "m", "prompt": "hi"}')
        request.headers = Headers({})
        await router.route(request)
        assert router._pick_replica.call_args.kwargs["request_token_ids"] is None


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
