"""Tests for openai_api_models.py engine-agnostic import behaviour.

SGLang fallback tests live in the llm_serve_sglang_e2e release test.
"""

import importlib
import sys

import pytest

_OAI_MODELS_MOD = "ray.llm._internal.serve.core.configs.openai_api_models"


class _VLLMImportBlocker:
    """Meta-path finder that makes every ``vllm.*`` import raise."""

    def find_module(self, fullname, path=None):
        if fullname == "vllm" or fullname.startswith("vllm."):
            return self
        return None

    def load_module(self, fullname):
        raise ImportError(f"Mocked: {fullname} is not installed")


class TestVLLMBackend:
    def test_wrapper_classes_inherit_from_vllm(self):
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            ChatCompletionRequest,
            CompletionRequest,
            ErrorInfo,
            ErrorResponse,
        )

        assert "vllm" in ChatCompletionRequest.__mro__[1].__module__
        assert "vllm" in CompletionRequest.__mro__[1].__module__
        assert "vllm" in ErrorInfo.__mro__[1].__module__
        assert "vllm" in ErrorResponse.__mro__[1].__module__

    def test_error_response_round_trip(self):
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            ErrorInfo,
            ErrorResponse,
        )

        info = ErrorInfo(message="something broke", code=500, type="InternalError")
        resp = ErrorResponse(error=info)
        assert resp.error.message == "something broke"
        assert resp.error.code == 500
        assert resp.error.type == "InternalError"

        dumped = resp.model_dump()
        assert dumped["error"]["message"] == "something broke"

    def test_transcription_request_has_request_id(self):
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            TranscriptionRequest,
        )

        assert "request_id" in TranscriptionRequest.model_fields

    def test_import_error_when_vllm_blocked(self):
        """SGLang is not installed here either, so blocking vLLM means neither
        backend is available."""
        blocker = _VLLMImportBlocker()
        saved = {
            k: sys.modules.pop(k)
            for k in list(sys.modules)
            if k == "vllm" or k.startswith("vllm.")
        }
        sys.modules.pop(_OAI_MODELS_MOD, None)
        sys.meta_path.insert(0, blocker)
        try:
            with pytest.raises(ImportError, match="Neither vLLM nor SGLang"):
                importlib.import_module(_OAI_MODELS_MOD)
        finally:
            sys.meta_path.remove(blocker)
            sys.modules.pop(_OAI_MODELS_MOD, None)
            sys.modules.update(saved)


class TestSanitizeChatCompletionRequest:
    def test_serializes_tool_calls_iterator(self):
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            ChatCompletionRequest,
        )
        from ray.llm._internal.serve.core.ingress.ingress import (
            _sanitize_chat_completion_request,
        )

        tool_calls = [
            {
                "id": "call_1",
                "type": "function",
                "function": {"name": "f", "arguments": "{}"},
            },
        ]

        request = ChatCompletionRequest(
            model="test-model",
            messages=[
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": None, "tool_calls": iter(tool_calls)},
            ],
        )

        result = _sanitize_chat_completion_request(request)
        assistant_msg = result.messages[1]
        assert isinstance(assistant_msg["tool_calls"], list)
        assert len(assistant_msg["tool_calls"]) == 1

    def test_handles_no_tool_calls(self):
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            ChatCompletionRequest,
        )
        from ray.llm._internal.serve.core.ingress.ingress import (
            _sanitize_chat_completion_request,
        )

        request = ChatCompletionRequest(
            model="test-model",
            messages=[
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "world"},
            ],
        )

        result = _sanitize_chat_completion_request(request)
        assert result is request


class TestLLMServerLazyImport:
    def test_default_engine_cls_is_none(self):
        mod_name = "ray.llm._internal.serve.core.server.llm_server"
        old_mod = sys.modules.pop(mod_name, None)
        try:
            mod = importlib.import_module(mod_name)
            assert mod.LLMServer._default_engine_cls is None
        finally:
            if old_mod is not None:
                sys.modules[mod_name] = old_mod

    def test_ray_serve_llm_importable(self):
        import ray.serve.llm  # noqa: F401
