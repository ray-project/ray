"""Correctness tests for in-process pre-routing tokenization.

The in-process tokenizer must produce exactly the token ids vLLM's
``/tokenize`` endpoint produces: KV-aware routing scores replicas on prompt
prefix overlap, so a divergence silently mis-routes every request. These tests
cross-validate the vLLM-renderer implementation against an independent ground
truth (raw ``transformers``) on a real tokenizer, and pin the endpoint's
parameter semantics (special-token defaults, ``add_generation_prompt``,
``chat_template_kwargs`` passthrough, untrusted-template refusal).
"""

import json
import sys

import pytest
from transformers import AutoTokenizer

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
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
        ids = await tokenizer.tokenize({"model": "test-model", "messages": messages})
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
            }
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
            }
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
                }
            )
        assert e.value.status_code == 400


class TestCompletionExactness:
    async def test_matches_transformers_ground_truth(self, tokenizer, hf_tokenizer):
        prompt = "The capital of France is"
        ids = await tokenizer.tokenize({"model": "test-model", "prompt": prompt})
        # /tokenize completion default: add_special_tokens=True.
        assert ids == hf_tokenizer.encode(prompt, add_special_tokens=True)

    async def test_add_special_tokens_false(self, tokenizer, hf_tokenizer):
        prompt = "plain continuation"
        ids = await tokenizer.tokenize(
            {"model": "test-model", "prompt": prompt, "add_special_tokens": False}
        )
        assert ids == hf_tokenizer.encode(prompt, add_special_tokens=False)


_BASH_TOOL_SCHEMA = {
    "type": "object",
    "properties": {"command": {"type": "string"}},
    "required": ["command"],
}


def _claude_code_body(billing_hash: str = "1a2b3") -> dict:
    """A Claude Code /v1/messages body: billing-header-led system prompt,
    Anthropic tool definitions, and a tool_use/tool_result turn."""
    return {
        "model": "test-model",
        "max_tokens": 1024,
        "stream": True,
        "system": [
            {
                "type": "text",
                "text": f"x-anthropic-billing-header: cch={billing_hash};",
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


# The OpenAI chat request vLLM's /v1/messages handler converts the body above
# into, written out by hand.
_EQUIVALENT_CHAT_BODY = {
    "model": "test-model",
    "messages": [
        {
            "role": "system",
            "content": "You are Claude Code. Use tools to inspect the repo.",
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
    async def test_matches_equivalent_chat_request(self, tool_tokenizer):
        """A Claude Code body routes on exactly the prompt of the OpenAI chat
        request the engine's /v1/messages handler renders it as."""
        ids = await tool_tokenizer.tokenize(_claude_code_body())
        assert ids
        assert ids == await tool_tokenizer.tokenize(_EQUIVALENT_CHAT_BODY)

    async def test_billing_header_does_not_change_ids(self, tool_tokenizer):
        """Claude Code's billing header hash changes per request; dropping it
        keeps every turn of a session on the same prefix."""
        ids = await tool_tokenizer.tokenize(_claude_code_body("1a2b3"))
        assert ids
        assert ids == await tool_tokenizer.tokenize(_claude_code_body("9f8e7"))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
