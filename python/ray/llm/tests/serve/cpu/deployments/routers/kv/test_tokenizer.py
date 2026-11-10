import asyncio
import copy
import json
import sys
from types import SimpleNamespace
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from fastapi import HTTPException
from starlette.datastructures import Headers
from vllm.entrypoints.openai.chat_completion.protocol import ChatCompletionRequest
from vllm.entrypoints.openai.completion.protocol import CompletionRequest

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_openai_app,
)
from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.routing_policies.kv_aware import kv_token_tracker
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
)
from ray.llm._internal.serve.routing_policies.kv_aware.router_state import KVRouterState
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.tokenizer import (
    TokenizeError,
    build_tokenize_request,
)
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
)
from ray.serve.experimental.round_robin_router import RoundRobinRouter
from ray.serve.llm.request_router import KVAwareRouter

_BASH_INPUT_SCHEMA = {
    "type": "object",
    "properties": {"command": {"type": "string"}},
    "required": ["command"],
}

# Anthropic request with an inline system message and a tool turn.
CLAUDE_CODE_BODY = {
    "model": "m",
    "max_tokens": 64,
    "system": "You are Claude Code.",
    "messages": [
        {"role": "user", "content": [{"type": "text", "text": "List the files."}]},
        {"role": "system", "content": "Plan mode is off."},
        {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "Listing them."},
                {
                    "type": "tool_use",
                    "id": "toolu_01",
                    "name": "Bash",
                    "input": {"command": "ls"},
                },
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "toolu_01",
                    "content": "a.py\nb.py",
                }
            ],
        },
    ],
    "tools": [
        {
            "name": "Bash",
            "description": "Run a shell command.",
            "input_schema": _BASH_INPUT_SCHEMA,
        }
    ],
}


class TestBuildTokenizeRequest:
    def test_converts_anthropic_messages_body(self):
        """Convert Claude Code's system prompt and tools using vLLM's converter."""
        request = build_tokenize_request(CLAUDE_CODE_BODY, request_path="/v1/messages")
        assert request is not None
        # Merge the inline system message, matching the engine's conversion.
        assert [m["role"] for m in request.messages] == [
            "system",
            "user",
            "assistant",
            "tool",
        ]
        assert request.messages[0] == {
            "role": "system",
            "content": "You are Claude Code.Plan mode is off.",
        }
        assert request.messages[2]["tool_calls"][0]["function"] == {
            "name": "Bash",
            "arguments": json.dumps({"command": "ls"}),
        }
        assert request.messages[3] == {
            "role": "tool",
            "tool_call_id": "toolu_01",
            "content": "a.py\nb.py",
        }
        assert request.tools[0].function.name == "Bash"
        assert request.tools[0].function.parameters == _BASH_INPUT_SCHEMA
        assert request.tool_choice == "auto"

    @pytest.mark.parametrize(
        "request_path", [None, "/app/v1/messages/count_tokens", "/tokenize", "/unknown"]
    )
    def test_missing_or_non_generation_path_returns_none(self, request_path):
        # A generation-shaped body must not override the actual endpoint.
        assert (
            build_tokenize_request(CLAUDE_CODE_BODY, request_path=request_path) is None
        )

    @pytest.mark.parametrize("stream", [True, False])
    @pytest.mark.parametrize(
        "params",
        [
            {"logprobs": True, "top_logprobs": -2},
            {"logprobs": False, "top_logprobs": 5},
        ],
    )
    def test_invalid_chat_sampling_params(self, stream, params):
        # vLLM's validators raise VLLMValidationError, which is not a
        # pydantic ValidationError. Let the engine report the bad request
        # instead of failing the HAProxy router consultation with a 500.
        assert (
            build_tokenize_request(
                {
                    "model": "m",
                    "messages": [{"role": "user", "content": "hi"}],
                    "stream": stream,
                    **params,
                },
                request_path="/v1/chat/completions",
            )
            is None
        )

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
        assert build_tokenize_request(payload, request_path="/v1/completions") is None

    @pytest.mark.parametrize(
        "request_path, payload, expected_request_type",
        [
            (
                "/v1/chat/completions",
                {"model": "m", "messages": [{"role": "user", "content": "hi"}]},
                ChatCompletionRequest,
            ),
            (
                "/v1/completions",
                {
                    "model": "m",
                    "prompt": "hello",
                    "messages": [{"role": "user", "content": "ignored"}],
                },
                CompletionRequest,
            ),
        ],
    )
    def test_builds_chat_and_completion_requests(
        self, request_path, payload, expected_request_type
    ):
        """Use the generation endpoint's native request model."""
        assert isinstance(
            build_tokenize_request(payload, request_path=request_path),
            expected_request_type,
        )

    @pytest.mark.parametrize(
        "request_path, payload, expected",
        [
            (  # chat: template-rendering fields + request-provided prompt flags
                "/v1/chat/completions",
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
                "/v1/completions",
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
    def test_preserves_prompt_fields(self, request_path, payload, expected):
        """Prompt-rendering fields use the engine's schema and defaults."""
        request = build_tokenize_request(payload, request_path=request_path)
        for attr, value in expected.items():
            assert getattr(request, attr) == value


@pytest.fixture
def tokenizer():
    return MagicMock(tokenize=AsyncMock(return_value=[5, 6, 7]))


@pytest_asyncio.fixture
async def kv_state(tokenizer):
    state = KVRouterState.__new__(KVRouterState)
    state.tokenizer_task = asyncio.create_task(asyncio.sleep(0, result=tokenizer))
    state.token_sender = MagicMock()
    state._warned_no_token_endpoint = False
    yield state
    state.close()


class TestRoute:
    @pytest.mark.asyncio
    async def test_no_tokenizer_forwards_no_token_ids(self):
        # A non-KV router has no tokenizer, so route forwards request_token_ids=None.
        router = LLMRouter.__new__(LLMRouter)
        router._handle = MagicMock()
        router._kv_state = None
        router._pick_replica = AsyncMock(return_value=("h", 1, "rid", None))

        request = MagicMock()
        request.body = AsyncMock(return_value=b'{"model": "m", "prompt": "hi"}')
        request.headers = Headers({})
        await router.route(request)
        assert router._pick_replica.call_args.kwargs["request_token_ids"] is None

    @pytest.mark.asyncio
    async def test_forwards_token_ids(self, kv_state):
        # A successful tokenization forwards its token ids to _pick_replica.
        router = LLMRouter.__new__(LLMRouter)
        router._handle = MagicMock()
        router._kv_state = kv_state
        router._pick_replica = AsyncMock(return_value=("h", 1, "rid", None))

        request = MagicMock()
        request.body = AsyncMock(return_value=b'{"model": "m", "prompt": "hi"}')
        request.headers = Headers({})
        await router.route(request)
        assert router._pick_replica.call_args.kwargs["request_token_ids"] == [5, 6, 7]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("push_succeeds", [True, False])
    async def test_route_token_header(self, kv_state, push_succeeds):
        router = LLMRouter.__new__(LLMRouter)
        router._handle = MagicMock()
        router._kv_state = kv_state
        router._pick_replica = AsyncMock(
            return_value=("h", 1, "rid", "tcp://127.0.0.1:7557")
        )
        kv_state.token_sender.push.return_value = push_succeeds

        request = MagicMock()
        request.body = AsyncMock(return_value=b'{"model": "m", "prompt": "hi"}')
        request.headers = Headers(
            {SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER: "/v1/completions"}
        )
        response = await router.route(request)

        kv_state.token_sender.push.assert_called_once()
        if push_succeeds:
            assert response[RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD] == {
                KV_TOKEN_KEY_HEADER: kv_state.token_sender.push.call_args.args[1]
            }
        else:
            assert RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD not in response

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "media, should_stage",
        [
            (None, True),
            ("image", False),
            ("tool_result_image", False),
            ("unknown_block", False),
            ("malformed_text", False),
        ],
    )
    async def test_anthropic_token_staging(
        self, kv_state, tokenizer, media, should_stage
    ):
        # Images can also appear inside tool results; tokens alone cannot
        # replace media preprocessing at the engine.
        router = LLMRouter.__new__(LLMRouter)
        router._handle = MagicMock()
        router._kv_state = kv_state
        router._pick_replica = AsyncMock(
            return_value=("h", 1, "rid", "tcp://127.0.0.1:7557")
        )

        payload = copy.deepcopy(CLAUDE_CODE_BODY)
        image = {
            "type": "image",
            "source": {"type": "url", "url": "https://example.com/image.png"},
        }
        if media == "image":
            payload["messages"][0]["content"].append(image)
        elif media == "tool_result_image":
            payload["messages"][-1]["content"][0]["content"] = [
                {"type": "text", "text": "a.py"},
                image,
            ]
        elif media == "unknown_block":
            payload["messages"][0]["content"].append({"type": "unknown"})
        elif media == "malformed_text":
            payload["messages"][0]["content"][0]["text"] = image
        request = MagicMock()
        request.body = AsyncMock(return_value=json.dumps(payload).encode())
        request.headers = Headers(
            {SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER: "/v1/messages"}
        )
        response = await router.route(request)

        tokenizer.tokenize.assert_awaited_once_with(
            payload, request_path="/v1/messages"
        )
        assert router._pick_replica.call_args.kwargs["request_token_ids"] == [5, 6, 7]
        if should_stage:
            kv_state.token_sender.push.assert_called_once()
            assert response[RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD] == {
                KV_TOKEN_KEY_HEADER: kv_state.token_sender.push.call_args.args[1]
            }
        else:
            kv_state.token_sender.push.assert_not_called()
            assert RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD not in response

    @pytest.mark.asyncio
    async def test_unparseable_body_skips_tokenization(self, kv_state, tokenizer):
        # A truncated/unparseable body derives no routing payload, so the
        # tokenizer is never called and request_token_ids stays None.
        router = LLMRouter.__new__(LLMRouter)
        router._handle = MagicMock()
        router._kv_state = kv_state
        router._pick_replica = AsyncMock(return_value=("h", 1, "rid", None))

        request = MagicMock()
        # Truncated prefix: not valid JSON, so it can't be parsed or tokenized.
        request.body = AsyncMock(return_value=b'{"model": "m", "prompt": "' + b"x" * 8)
        request.headers = Headers({"x-body-truncated": "8/90000"})
        await router.route(request)

        tokenizer.tokenize.assert_not_called()
        assert router._pick_replica.call_args.kwargs["request_token_ids"] is None

    @pytest.mark.asyncio
    async def test_tokenize_error_becomes_http_error(self, kv_state, tokenizer):
        # A /tokenize rejection becomes an HTTPException with the same status
        # code, and routing is not attempted.
        router = LLMRouter.__new__(LLMRouter)
        router._handle = MagicMock()
        router._kv_state = kv_state
        tokenizer.tokenize = AsyncMock(
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


class TestKVRouterState:
    @pytest.mark.asyncio
    async def test_request_cancellation_preserves_tokenizer_startup(
        self, kv_state, tokenizer
    ):
        await kv_state.tokenizer_task
        ready = asyncio.Event()

        async def load_tokenizer():
            await ready.wait()
            return tokenizer

        kv_state.tokenizer_task = asyncio.create_task(load_tokenizer())
        payload = SimpleNamespace(model="m", prompt="hi")
        request = asyncio.create_task(
            kv_state.tokenize(payload, request_path="/v1/completions")
        )
        await asyncio.sleep(0)
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert not kv_state.tokenizer_task.cancelled()

        ready.set()
        assert await kv_state.tokenize(payload, request_path="/v1/completions") == [
            5,
            6,
            7,
        ]

    @pytest.mark.asyncio
    async def test_tokenizer_startup_error_fails_health(self, kv_state):
        await kv_state.tokenizer_task

        async def fail():
            raise ValueError("invalid tokenizer config")

        kv_state.tokenizer_task = asyncio.create_task(fail())
        with pytest.raises(ValueError, match="invalid tokenizer config"):
            await kv_state.tokenizer_task
        with pytest.raises(ValueError, match="invalid tokenizer config"):
            kv_state.check_health()

    @pytest.mark.asyncio
    async def test_llm_router_registers_and_subscribes_before_initializing_handle(
        self, monkeypatch, tokenizer
    ):
        from ray.llm._internal.serve.routing_policies.kv_aware import token_channel
        from ray.llm._internal.serve.routing_policies.kv_aware.vllm import (
            tokenizer as tokenizer_module,
        )

        events = []
        deployment_id = DeploymentID("LLMServer", app_name="model")
        tracker = MagicMock(_serve_deployment_id=deployment_id)

        def build_tracker(**kwargs):
            events.append("tracker")
            assert kwargs["serve_deployment_id"] == deployment_id
            return tracker

        def load_tokenizer(config):
            events.append("tokenizer")
            return tokenizer

        def initialize_handle():
            events.append("handle")
            assert kv_token_tracker.get_kv_token_tracker(deployment_id) is tracker

        tracker.subscribe_token_endpoints.side_effect = lambda callback: events.append(
            "subscribe"
        )
        monkeypatch.setattr(kv_token_tracker, "KVTokenTracker", build_tracker)
        monkeypatch.setattr(kv_token_tracker, "get_llm_router_handle", MagicMock())
        monkeypatch.setattr(tokenizer_module, "Tokenizer", load_tokenizer)
        monkeypatch.setattr(token_channel, "TokenSender", MagicMock)
        handle = MagicMock(deployment_id=deployment_id)
        handle._init.side_effect = initialize_handle
        config = LLMConfig(model_loading_config={"model_id": "m"})
        router = LLMRouter.__new__(LLMRouter)
        kv_token_tracker.set_kv_token_tracker(None)
        try:
            await LLMRouter.__bases__[0].__init__(router, handle, llm_config=config)
            assert isinstance(router._kv_state, KVRouterState)
            assert events == ["tracker", "subscribe", "tokenizer", "handle"]
            tracker.subscribe_token_endpoints.assert_called_once_with(
                router._kv_state.token_sender.connect
            )
        finally:
            kv_token_tracker.set_kv_token_tracker(None)


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
        for name, value in llm_config.runtime_env["env_vars"].items():
            assert runtime_env["env_vars"][name] == value
        assert (
            runtime_env["env_vars"]["RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD"] == "0"
        )
        assert runtime_env["env_vars"]["RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP"] == "0"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
