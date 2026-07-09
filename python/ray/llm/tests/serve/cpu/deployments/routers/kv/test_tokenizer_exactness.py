"""Correctness tests for in-process pre-routing tokenization.

The in-process tokenizer must produce exactly the token ids vLLM's
``/tokenize`` endpoint produces: KV-aware routing scores replicas on prompt
prefix overlap, so a divergence silently mis-routes every request. These tests
cross-validate the vLLM-renderer implementation against an independent ground
truth (raw ``transformers``) on a real tokenizer, and pin the endpoint's
parameter semantics (special-token defaults, ``add_generation_prompt``,
``chat_template_kwargs`` passthrough, untrusted-template refusal).
"""

import sys

import pytest
from transformers import AutoTokenizer

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.tokenizer import (
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


@pytest.fixture(scope="module")
def llm_config() -> LLMConfig:
    return LLMConfig(
        model_loading_config=dict(model_id="test-model", model_source=MODEL),
        engine_kwargs=dict(max_model_len=4096, enforce_eager=True),
    )


@pytest.fixture(scope="module")
def tokenizer(llm_config) -> Tokenizer:
    # Module-scoped: construction resolves the engine config and builds the
    # vLLM renderer once for all tests.
    return Tokenizer(llm_config)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
