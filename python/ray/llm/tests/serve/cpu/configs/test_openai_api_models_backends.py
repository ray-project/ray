"""Tests for openai_api_models.py engine-agnostic import behaviour.

SGLang fallback tests live in the llm_serve_sglang_e2e release test.
"""

import importlib
import sys
from contextlib import contextmanager

import pytest

_OAI_MODELS_MOD = "ray.llm._internal.serve.core.configs.openai_api_models"


class _VLLMImportBlocker:
    """Meta-path finder that simulates vLLM not being installed.

    Raises ModuleNotFoundError with .name set to mirror what Python raises
    when a package is genuinely absent, so raise_llm_engine_import_error
    can distinguish "not installed" from "installed but broken".
    """

    def find_spec(self, fullname, path=None, target=None):
        if fullname == "vllm" or fullname.startswith("vllm."):
            err = ModuleNotFoundError(f"Mocked: {fullname} is not installed")
            err.name = fullname
            raise err
        return None


class _LLMEngineImportBlocker:
    """Meta-path finder that simulates both LLM engines being absent."""

    def find_spec(self, fullname, path=None, target=None):
        for package in ("vllm", "sglang"):
            if fullname == package or fullname.startswith(f"{package}."):
                err = ModuleNotFoundError(f"Mocked: {fullname} is not installed")
                err.name = package
                raise err
        return None


class _VLLMBrokenInstallBlocker:
    """Meta-path finder that simulates vLLM installed but broken at runtime
    (e.g. missing libcudart.so or a missing transitive dependency).
    """

    def __init__(self, error: ImportError):
        self._error = error

    def find_spec(self, fullname, path=None, target=None):
        if fullname == "vllm" or fullname.startswith("vllm."):
            raise self._error
        return None


class TestVLLMBackend:
    def test_wrapper_classes_inherit_from_vllm(self):
        pytest.importorskip("vllm")

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

    def _reload_oai_models_with_blocker(self, blocker):
        """Helper: evict vllm + the target module, install blocker, reimport."""
        saved = {
            k: sys.modules.pop(k)
            for k in list(sys.modules)
            if k == "vllm" or k.startswith("vllm.")
        }
        sys.modules.pop(_OAI_MODELS_MOD, None)
        sys.meta_path.insert(0, blocker)
        try:
            importlib.import_module(_OAI_MODELS_MOD)
        finally:
            sys.meta_path.remove(blocker)
            sys.modules.pop(_OAI_MODELS_MOD, None)
            sys.modules.update(saved)

    def test_fallback_when_vllm_blocked(self):
        """The dependency-free fallback is used when neither engine exists."""
        self._reload_oai_models_with_blocker(_VLLMImportBlocker())

    def test_vllm_installed_but_broken_cuda(self):
        """Plain ImportError (e.g. missing libcudart.so) → clear message that
        vLLM is installed but failed to load, not 'not installed'."""
        cuda_err = ImportError(
            "libcudart.so.12: cannot open shared object file: No such file or directory"
        )
        blocker = _VLLMBrokenInstallBlocker(cuda_err)
        with pytest.raises(ImportError, match="vLLM is installed but failed to import"):
            self._reload_oai_models_with_blocker(blocker)

    def test_vllm_installed_but_missing_transitive_dep(self):
        """ModuleNotFoundError for a *dependency* of vLLM (not vllm itself)
        must also be reported as 'installed but broken', not 'not installed'."""
        dep_err = ModuleNotFoundError("No module named 'msgpack'")
        dep_err.name = "msgpack"
        blocker = _VLLMBrokenInstallBlocker(dep_err)
        with pytest.raises(ImportError, match="vLLM is installed but failed to import"):
            self._reload_oai_models_with_blocker(blocker)


class TestNoEngineBackend:
    @contextmanager
    def _load_without_engines(self):
        saved = {
            key: sys.modules.pop(key)
            for key in list(sys.modules)
            if key in {"vllm", "sglang"}
            or key.startswith("vllm.")
            or key.startswith("sglang.")
        }
        old_module = sys.modules.pop(_OAI_MODELS_MOD, None)
        blocker = _LLMEngineImportBlocker()
        sys.meta_path.insert(0, blocker)
        try:
            yield importlib.import_module(_OAI_MODELS_MOD)
        finally:
            sys.meta_path.remove(blocker)
            sys.modules.pop(_OAI_MODELS_MOD, None)
            if old_module is not None:
                sys.modules[_OAI_MODELS_MOD] = old_module
            sys.modules.update(saved)

    def test_import_and_engine_independent_models(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        with self._load_without_engines() as models:
            request = models.ChatCompletionRequest(
                model="test-model",
                messages=[{"role": "user", "content": "hello"}],
                stream=False,
            )
            assert request.model == "test-model"
            assert request.model_dump()["messages"][0]["content"] == "hello"

            error = models.ErrorResponse(
                error=models.ErrorInfo(
                    message="something broke", code=500, type="InternalError"
                )
            )
            assert error.model_dump()["error"]["message"] == "something broke"

            app = FastAPI()

            @app.post("/v1/chat/completions")
            async def chat(body: models.ChatCompletionRequest):
                return body.model_dump()

            response = TestClient(app).post(
                "/v1/chat/completions",
                json={
                    "model": "test-model",
                    "messages": [{"role": "user", "content": "hello"}],
                    "temperature": 0.5,
                },
            )
            assert response.status_code == 200
            assert response.json()["temperature"] == 0.5

            response = TestClient(app).post(
                "/v1/chat/completions", json={"model": "test-model"}
            )
            assert response.status_code == 422

    def test_transport_round_trip(self):
        import pickle

        from ray.llm._internal.serve.core.protocol import (
            deserialize_openai_model,
            serialize_openai_model,
        )
        from ray.llm._internal.serve.core.server.llm_server import (
            _deserialize_openai_request,
        )

        with self._load_without_engines() as models:
            request = models.CompletionRequest(
                model="test-model", prompt="hello", stream=False
            )
            payload = pickle.loads(pickle.dumps(serialize_openai_model(request)))

        restored = _deserialize_openai_request(payload)
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            CompletionRequest,
        )

        assert isinstance(restored, CompletionRequest)
        assert restored.model_dump() == request.model_dump()

        transported = serialize_openai_model((request, "stream chunk"))
        restored = deserialize_openai_model(
            transported, {"CompletionRequest": CompletionRequest}
        )
        assert isinstance(restored, tuple)
        assert isinstance(restored[0], CompletionRequest)
        assert restored[1] == "stream chunk"


class TestIngressTransport:
    def test_request_and_response_cross_dependency_boundary(self):
        import asyncio

        from ray.llm._internal.serve.core.configs.openai_api_models import (
            CompletionRequest,
            ErrorInfo,
            ErrorResponse,
        )
        from ray.llm._internal.serve.core.ingress.ingress import OpenAiIngress
        from ray.llm._internal.serve.core.protocol import (
            OpenAIModelPayload,
            serialize_openai_model,
        )

        class _RemoteMethod:
            request = None

            def remote(self, request, raw_request_info):
                self.request = request

                async def generate():
                    yield serialize_openai_model(
                        ErrorResponse(
                            error=ErrorInfo(
                                message="something broke",
                                code=500,
                                type="InternalError",
                            )
                        )
                    )

                return generate()

        class _Handle:
            completions = _RemoteMethod()

            def options(self, **kwargs):
                return self

        handle = _Handle()
        ingress = OpenAiIngress({"test-model": handle}, {"test-model": object()})
        request = CompletionRequest(model="test-model", prompt="hello")

        async def collect():
            return [
                item
                async for item in ingress._get_response(
                    body=request, call_method="completions"
                )
            ]

        responses = asyncio.run(collect())

        assert isinstance(handle.completions.request, OpenAIModelPayload)
        assert handle.completions.request.model_type == "CompletionRequest"
        assert handle.completions.request.data["prompt"] == "hello"
        assert isinstance(responses[0], ErrorResponse)
        assert responses[0].error.message == "something broke"


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
