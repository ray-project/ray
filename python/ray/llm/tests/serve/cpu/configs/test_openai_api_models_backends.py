"""Tests for openai_api_models.py engine-agnostic import behaviour.

SGLang fallback tests live in the llm_serve_sglang_e2e release test.
"""

import importlib
import subprocess
import sys
import textwrap

import pytest

_OAI_MODELS_MOD = "ray.llm._internal.serve.core.configs.openai_api_models"


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
        backend is available.

        Run in a subprocess: vLLM (>=0.20) calls ``register_opaque_type`` at
        module-load time, which writes to torch's C-side global registry.
        Popping ``vllm.*`` from ``sys.modules`` and re-importing in the same
        process would attempt to re-register and raise ``RuntimeError``.
        """
        code = textwrap.dedent(
            f"""
            import importlib
            import importlib.util
            import sys

            class _VLLMImportBlocker:
                def find_spec(self, fullname, path, target=None):
                    if fullname == "vllm" or fullname.startswith("vllm."):
                        return importlib.util.spec_from_loader(fullname, self)
                    return None

                def create_module(self, spec):
                    return None

                def exec_module(self, module):
                    raise ImportError(
                        f"Mocked: {{module.__name__}} is not installed"
                    )

            sys.meta_path.insert(0, _VLLMImportBlocker())
            try:
                importlib.import_module({_OAI_MODELS_MOD!r})
            except ImportError as e:
                assert "Neither vLLM nor SGLang" in str(e), str(e)
                sys.exit(0)
            sys.exit(1)
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, (
            f"returncode={result.returncode}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )


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
