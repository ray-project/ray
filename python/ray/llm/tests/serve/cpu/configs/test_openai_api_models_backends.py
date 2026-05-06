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


class _PartialVLLMBlocker:
    """Block only the listed ``vllm.*`` submodules; leave the rest importable.

    Simulates a partial / dev vLLM build (e.g. the DeepSeek-V4 cu130 image)
    that exposes chat/completion/error but is missing newer optional protocol
    submodules like ``vllm.entrypoints.pooling.score``.
    """

    def __init__(self, blocked):
        self.blocked = tuple(blocked)

    def find_module(self, fullname, path=None):
        if fullname in self.blocked or any(
            fullname.startswith(b + ".") for b in self.blocked
        ):
            return self
        return None

    def load_module(self, fullname):
        raise ImportError(f"Mocked: {fullname} is not installed")


class TestPartialVLLM:
    """Cover the case where vLLM is installed but missing optional submodules.

    Before this fix, a single missing submodule (e.g. pooling.score) caused the
    whole openai_api_models import to fall through to the SGLang branch — which
    fails entirely if SGLang is also absent. Result: ray.serve.llm became
    unusable on otherwise-working vLLM builds.

    After the fix, each feature group is resolved independently. Missing
    optional groups become NotImplementedError stubs; chat/completion still
    work.
    """

    def _reimport_with_blocked(self, blocked):
        blocker = _PartialVLLMBlocker(blocked)
        saved = {
            k: sys.modules.pop(k)
            for k in list(sys.modules)
            if k in blocked or any(k.startswith(b + ".") for b in blocked)
        }
        sys.modules.pop(_OAI_MODELS_MOD, None)
        sys.meta_path.insert(0, blocker)
        try:
            return importlib.import_module(_OAI_MODELS_MOD)
        finally:
            sys.meta_path.remove(blocker)
            sys.modules.pop(_OAI_MODELS_MOD, None)
            sys.modules.update(saved)

    def test_import_succeeds_when_pooling_score_missing(self):
        """vLLM cu130 dev build (DS-V4) is missing pooling.score — must still import."""
        mod = self._reimport_with_blocked(["vllm.entrypoints.pooling.score"])
        assert "vllm" in mod.ChatCompletionRequest.__mro__[1].__module__
        assert "vllm" in mod.CompletionRequest.__mro__[1].__module__

    def test_score_endpoints_stub_raise_when_unavailable(self):
        mod = self._reimport_with_blocked(["vllm.entrypoints.pooling.score"])
        with pytest.raises(NotImplementedError, match="ScoreTextRequest"):
            mod.ScoreRequest(model="m", text_1="a", text_2="b")

    def test_import_succeeds_when_pooling_embed_missing(self):
        mod = self._reimport_with_blocked(["vllm.entrypoints.pooling.embed"])
        assert "vllm" in mod.ChatCompletionRequest.__mro__[1].__module__
        with pytest.raises(NotImplementedError, match="Embedding"):
            mod.EmbeddingResponse(data=[])

    def test_import_succeeds_when_speech_to_text_missing(self):
        mod = self._reimport_with_blocked(
            ["vllm.entrypoints.openai.speech_to_text"]
        )
        with pytest.raises(NotImplementedError, match="Transcription"):
            mod.TranscriptionResponse()

    def test_import_succeeds_when_tokenize_missing(self):
        mod = self._reimport_with_blocked(["vllm.entrypoints.serve.tokenize"])
        with pytest.raises(NotImplementedError, match="Tokenize"):
            mod.TokenizeCompletionRequest(model="m", prompt="x")

    def test_chat_still_works_with_all_optionals_blocked(self):
        """A vLLM with only chat+completion+engine still works for DS-V4."""
        mod = self._reimport_with_blocked(
            [
                "vllm.entrypoints.pooling.score",
                "vllm.entrypoints.pooling.embed",
                "vllm.entrypoints.openai.speech_to_text",
                "vllm.entrypoints.serve.tokenize",
            ]
        )
        req = mod.ChatCompletionRequest(
            model="deepseek-v4-pro",
            messages=[{"role": "user", "content": "hi"}],
        )
        assert req.model == "deepseek-v4-pro"


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
        # Sanitizer must not inject tool_calls onto messages that never had them.
        assistant_msg = result.messages[1]
        if isinstance(assistant_msg, dict):
            assert assistant_msg.get("tool_calls") is None
        else:
            assert getattr(assistant_msg, "tool_calls", None) is None

    def test_serializes_content_iterator(self):
        """When `content` is sent as a list of content parts on any message,
        Pydantic stores it as a ValidatorIterator against the `Iterable[...]`
        arm and the request becomes unpicklable until sanitized."""
        import pickle

        from ray.llm._internal.serve.core.configs.openai_api_models import (
            ChatCompletionRequest,
        )
        from ray.llm._internal.serve.core.ingress.ingress import (
            _sanitize_chat_completion_request,
        )

        request = ChatCompletionRequest(
            model="test-model",
            messages=[
                {"role": "system", "content": [{"text": "sys", "type": "text"}]},
                {"role": "user", "content": [{"text": "hi", "type": "text"}]},
                {
                    "role": "assistant",
                    "content": [{"text": "step", "type": "text"}],
                },
                {
                    "role": "tool",
                    "content": [{"text": "r", "type": "text"}],
                    "tool_call_id": "c0",
                },
            ],
        )

        result = _sanitize_chat_completion_request(request)
        for msg in result.messages:
            assert isinstance(msg, dict)
            assert isinstance(msg["content"], list)
            assert type(msg["content"]).__name__ != "ValidatorIterator"

        pickle.dumps(result)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
