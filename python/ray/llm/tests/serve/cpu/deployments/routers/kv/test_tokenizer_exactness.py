"""Correctness tests for in-process pre-routing tokenization.

The in-process tokenizer must produce exactly the prompt token IDs vLLM's
generation endpoints use: KV-aware routing scores replicas on prompt
prefix overlap, so a divergence silently mis-routes every request. These tests
cross-validate the vLLM-renderer implementation against an independent ground
truth (raw ``transformers``) on a real tokenizer, and pin the endpoint's
parameter semantics (special-token defaults, ``add_generation_prompt``,
``chat_template_kwargs`` passthrough, untrusted-template refusal).
"""

import json
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from starlette.datastructures import Headers
from transformers import AutoTokenizer
from vllm.entrypoints.anthropic.protocol import AnthropicMessagesRequest
from vllm.entrypoints.anthropic.serving import AnthropicServingMessages
from vllm.renderers.inputs.preprocess import extract_prompt_components

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
)
from ray.llm._internal.serve.routing_policies.kv_aware.token_channel import (
    TokenStore,
    encode_prompt_token_ids,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.prompt_token_forwarding import (
    install_prompt_token_forwarding,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.tokenizer import (
    TokenizeError,
    Tokenizer,
)

MODEL = "Qwen/Qwen3-0.6B"

# The renderer binds its async tokenizer to the running loop at first use, so
# every test must share one loop (as the LLMRouter replica does in production).
pytestmark = pytest.mark.asyncio(loop_scope="module")

CHAT_CASES = [
    pytest.param(
        [{"role": "user", "content": "What is the capital of France?"}],
        id="single_turn",
    ),
    pytest.param(
        [
            {"role": "system", "content": "You are terse."},
            {"role": "user", "content": "Hi!"},
            {"role": "assistant", "content": "Hello."},
            {"role": "user", "content": "Summarize our chat."},
        ],
        id="multi_turn_with_system",
    ),
    pytest.param(
        [{"role": "user", "content": "éèê 你好 \U0001f680 \n\t tabs/newlines"}],
        id="unicode_and_whitespace",
    ),
]


def _build_tokenizer(**engine_kwargs) -> Tokenizer:
    # A GPU-less CI node has no vLLM platform, so pin a device when none is
    # detected; tokenization needs only the model's tokenizer and chat template,
    # not a real device.
    from vllm.platforms import current_platform

    if not current_platform.device_type:
        current_platform.device_type = "cpu"
    return Tokenizer(
        LLMConfig(
            model_loading_config=dict(model_id="test-model", model_source=MODEL),
            engine_kwargs=dict(max_model_len=4096, enforce_eager=True, **engine_kwargs),
        )
    )


@pytest.fixture(scope="module")
def tokenizer() -> Tokenizer:
    # Module-scoped: construction resolves the engine config and builds the
    # vLLM renderer once for all tests.
    return _build_tokenizer()


@pytest.fixture(scope="module")
def tool_tokenizer() -> Tokenizer:
    """A deployment with tool calling enabled, as Claude Code requires."""
    return _build_tokenizer(enable_auto_tool_choice=True, tool_call_parser="hermes")


@pytest.fixture(scope="module")
def hf_tokenizer():
    return AutoTokenizer.from_pretrained(MODEL)


def _hf_chat_ids(hf_tokenizer, messages, add_generation_prompt=True, **kwargs):
    """Independent ground truth: raw transformers chat-template render +
    encode with add_special_tokens=False (the template adds special tokens),
    matching the /tokenize chat defaults."""
    text = hf_tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=add_generation_prompt,
        **kwargs,
    )
    return hf_tokenizer.encode(text, add_special_tokens=False)


class TestChatExactness:
    @pytest.mark.parametrize("messages", CHAT_CASES)
    async def test_matches_transformers_ground_truth(
        self, tokenizer, hf_tokenizer, messages
    ):
        ids = await tokenizer.tokenize(
            {"model": "test-model", "messages": messages},
            request_path="/v1/chat/completions",
        )
        assert ids == _hf_chat_ids(hf_tokenizer, messages)

    async def test_add_generation_prompt_false(self, tokenizer, hf_tokenizer):
        messages = [
            {"role": "user", "content": "Hi"},
            {"role": "assistant", "content": "Hello!"},
        ]
        ids = await tokenizer.tokenize(
            {
                "model": "test-model",
                "messages": messages,
                "add_generation_prompt": False,
            },
            request_path="/v1/chat/completions",
        )
        assert ids == _hf_chat_ids(hf_tokenizer, messages, add_generation_prompt=False)

    async def test_chat_template_kwargs_passthrough(self, tokenizer, hf_tokenizer):
        """Qwen3's enable_thinking template kwarg changes the rendered prompt;
        it must reach the template exactly as /tokenize forwards it."""
        messages = [{"role": "user", "content": "2+2?"}]
        ids = await tokenizer.tokenize(
            {
                "model": "test-model",
                "messages": messages,
                "chat_template_kwargs": {"enable_thinking": False},
            },
            request_path="/v1/chat/completions",
        )
        expected = _hf_chat_ids(hf_tokenizer, messages, enable_thinking=False)
        assert ids == expected
        assert ids != _hf_chat_ids(hf_tokenizer, messages)

    async def test_untrusted_request_chat_template_is_refused(self, tokenizer):
        """/tokenize refuses request-supplied chat templates unless the server
        opts in (--trust-request-chat-template); mirror the same 400."""
        with pytest.raises(TokenizeError) as e:
            await tokenizer.tokenize(
                {
                    "model": "test-model",
                    "messages": [{"role": "user", "content": "hi"}],
                    "chat_template": "{{ messages }}",
                },
                request_path="/v1/chat/completions",
            )
        assert e.value.status_code == 400


class TestCompletionExactness:
    @pytest.mark.parametrize("truncate_prompt_tokens", [None, 4])
    async def test_matches_transformers_ground_truth(
        self, tokenizer, hf_tokenizer, truncate_prompt_tokens
    ):
        prompt = "The capital of France is"
        ids = await tokenizer.tokenize(
            {
                "model": "test-model",
                "prompt": prompt,
                "truncate_prompt_tokens": truncate_prompt_tokens,
                # Extra chat fields must not select chat rendering when the
                # actual endpoint is completions.
                "messages": [{"role": "user", "content": "ignored"}],
            },
            request_path="/v1/completions",
        )
        expected = hf_tokenizer.encode(prompt, add_special_tokens=True)
        if truncate_prompt_tokens is not None:
            expected = expected[-truncate_prompt_tokens:]
        assert ids == expected

    async def test_add_special_tokens_false(self, tokenizer, hf_tokenizer):
        prompt = "plain continuation"
        ids = await tokenizer.tokenize(
            {"model": "test-model", "prompt": prompt, "add_special_tokens": False},
            request_path="/v1/completions",
        )
        assert ids == hf_tokenizer.encode(prompt, add_special_tokens=False)


_BASH_TOOL_SCHEMA = {
    "type": "object",
    "properties": {"command": {"type": "string"}},
    "required": ["command"],
}


def _claude_code_body() -> dict:
    """A Claude Code /v1/messages body: billing-header-led system prompt, an
    inline system reminder, Anthropic tool definitions, and a
    tool_use/tool_result turn."""
    return {
        "model": "test-model",
        "max_tokens": 1024,
        "stream": True,
        "system": [
            {
                "type": "text",
                "text": "x-anthropic-billing-header: cch=1a2b3;",
            },
            {"type": "text", "text": "You are Claude Code. "},
            {
                "type": "text",
                "text": "Use tools to inspect the repo.",
                "cache_control": {"type": "ephemeral"},
            },
        ],
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "List the files."}]},
            {"role": "system", "content": " Plan mode is off."},
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
                    },
                    {"type": "text", "text": "Which one is the entry point?"},
                ],
            },
        ],
        "tools": [
            {
                "name": "Bash",
                "description": "Run a shell command.",
                "input_schema": _BASH_TOOL_SCHEMA,
            }
        ],
    }


# Handwritten expected conversion, with the inline system message merged.
_EQUIVALENT_CHAT_BODY = {
    "model": "test-model",
    "messages": [
        {
            "role": "system",
            "content": (
                "You are Claude Code. Use tools to inspect the repo. "
                "Plan mode is off."
            ),
        },
        {"role": "user", "content": "List the files."},
        {
            "role": "assistant",
            "content": "Listing them.",
            "tool_calls": [
                {
                    "id": "toolu_01",
                    "type": "function",
                    "function": {
                        "name": "Bash",
                        "arguments": json.dumps({"command": "ls"}),
                    },
                }
            ],
        },
        {"role": "tool", "tool_call_id": "toolu_01", "content": "a.py\nb.py"},
        {"role": "user", "content": "Which one is the entry point?"},
    ],
    "tools": [
        {
            "type": "function",
            "function": {
                "name": "Bash",
                "description": "Run a shell command.",
                "parameters": _BASH_TOOL_SCHEMA,
            },
        }
    ],
    "tool_choice": "auto",
}


class TestAnthropicMessagesExactness:
    async def test_api_path_disambiguates_body(self, tokenizer, hf_tokenizer):
        # Both APIs accept this body, but Anthropic conversion moves the inline
        # system message and converts thinking to a reasoning field.
        messages = [
            {"role": "user", "content": "Hi."},
            {"role": "system", "content": "Be terse."},
            {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "Greet back."},
                    {"type": "text", "text": "Hello."},
                ],
            },
            {"role": "user", "content": "Again."},
        ]
        expected_messages = [
            {"role": "system", "content": "Be terse."},
            messages[0],
            {"role": "assistant", "content": "Hello.", "reasoning": "Greet back."},
            messages[-1],
        ]
        payload = {"model": "test-model", "max_tokens": 64, "messages": messages}
        ids = await tokenizer.tokenize(payload, request_path="/app/v1/messages")
        assert ids == _hf_chat_ids(hf_tokenizer, expected_messages)
        assert ids != await tokenizer.tokenize(
            payload, request_path="/app/v1/chat/completions"
        )

    async def test_matches_equivalent_chat_request(self, tool_tokenizer):
        """Claude Code and equivalent chat requests produce identical tokens."""
        ids = await tool_tokenizer.tokenize(
            _claude_code_body(), request_path="/v1/messages"
        )
        assert ids
        assert ids == await tool_tokenizer.tokenize(
            _EQUIVALENT_CHAT_BODY, request_path="/v1/chat/completions"
        )

    @pytest.mark.parametrize("staged", [True, False])
    async def test_anthropic_engine_reuses_tokens_or_falls_back(
        self, tool_tokenizer, monkeypatch, staged
    ):
        # Use real conversion and rendering; stub generation. Staged IDs skip
        # tokenization, while a staging miss must render the same IDs.
        body = _claude_code_body()
        ids = await tool_tokenizer.tokenize(body, request_path="/v1/messages")
        assert ids
        store = TokenStore()
        if staged:
            store.put("key", payload=encode_prompt_token_ids(ids))
        raw_request = SimpleNamespace(headers=Headers({KV_TOKEN_KEY_HEADER: "key"}))
        serving = AnthropicServingMessages.__new__(AnthropicServingMessages)
        serving._merge_inline_system = tool_tokenizer._merge_inline_system

        render = AsyncMock(wraps=tool_tokenizer._renderer.renderer.render_chat_async)
        monkeypatch.setattr(
            tool_tokenizer._renderer.renderer, "render_chat_async", render
        )

        generation_stub = tool_tokenizer._renderer.create_error_response(
            "generation stub"
        )

        async def generate(chat_request, raw_request=None):
            _, inputs = await tool_tokenizer._renderer.render_chat(chat_request)
            actual = [
                token
                for inp in inputs
                for token in extract_prompt_components(
                    tool_tokenizer._model_config, inp
                ).token_ids
            ]
            assert actual == ids
            return generation_stub

        serving.create_chat_completion = generate
        install_prompt_token_forwarding(
            SimpleNamespace(anthropic_serving_messages=serving), store
        )
        response = await serving.create_messages(
            AnthropicMessagesRequest.model_validate(body), raw_request
        )

        assert response is generation_stub
        if staged:
            render.assert_not_awaited()
        else:
            render.assert_awaited_once()
        assert store.pop("key") is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
