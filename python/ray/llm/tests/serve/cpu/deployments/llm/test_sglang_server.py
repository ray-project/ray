from types import SimpleNamespace

import pytest

from ray.llm._internal.serve.core.configs.openai_api_models import (
    DetokenizeRequest,
    TokenizeChatRequest,
    TokenizeCompletionRequest,
)
from ray.llm.examples.sglang.modules.sglang_engine import (
    SGLangServer,
    format_messages_to_prompt,
)
from ray.llm.tests.serve.utils.testing_utils import LLMResponseValidator


class _StubTokenizer:
    model_max_length = 8192

    def encode(self, text, add_special_tokens=True):
        return [ord(ch) for ch in text]

    def decode(self, tokens, skip_special_tokens=True):
        return "".join(chr(token) for token in tokens)

    def convert_ids_to_tokens(self, tokens):
        return [chr(token) for token in tokens]


class _StubEngine:
    def __init__(self, tokenizer):
        self.tokenizer_manager = SimpleNamespace(
            tokenizer=tokenizer,
            max_req_input_len=4096,
        )


def _make_server(engine):
    server = object.__new__(SGLangServer)
    server.engine = engine
    server._llm_config = None
    return server


class TestSGLangServerTokenization:
    @pytest.mark.asyncio
    async def test_tokenize_completion_request(self):
        server = _make_server(_StubEngine(_StubTokenizer()))
        request = TokenizeCompletionRequest(
            model="test-model",
            prompt="Hello, world!",
            add_special_tokens=False,
            return_token_strs=True,
        )

        async for response in server.tokenize(request):
            LLMResponseValidator.validate_tokenize_response(
                response,
                expected_prompt="Hello, world!",
                return_token_strs=True,
            )

    @pytest.mark.asyncio
    async def test_tokenize_chat_request_uses_chat_prompt_format(self):
        tokenizer = _StubTokenizer()
        server = _make_server(SimpleNamespace(get_tokenizer=lambda: tokenizer))
        request = TokenizeChatRequest(
            model="test-model",
            messages=[{"role": "user", "content": "Hello"}],
            add_special_tokens=False,
            return_token_strs=False,
        )
        expected_prompt = format_messages_to_prompt(request.messages)

        async for response in server.tokenize(request):
            assert response.tokens == [ord(ch) for ch in expected_prompt]
            assert response.count == len(expected_prompt)
            assert response.max_model_len == tokenizer.model_max_length

    @pytest.mark.asyncio
    async def test_detokenize_request(self):
        server = _make_server(_StubEngine(_StubTokenizer()))
        request = DetokenizeRequest(
            model="test-model",
            tokens=[72, 101, 108, 108, 111],
        )

        async for response in server.detokenize(request):
            LLMResponseValidator.validate_detokenize_response(
                response,
                expected_text="Hello",
            )
