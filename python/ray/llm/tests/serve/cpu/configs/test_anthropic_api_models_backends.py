"""Tests for anthropic_api_models.py vLLM import behaviour."""

import importlib
import sys

import pytest

_ANTHROPIC_MODELS_MOD = "ray.llm._internal.serve.core.configs.anthropic_api_models"


class _VLLMImportBlocker:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "vllm" or fullname.startswith("vllm."):
            err = ModuleNotFoundError(f"Mocked: {fullname} is not installed")
            err.name = fullname
            raise err
        return None


class _VLLMBrokenInstallBlocker:
    def __init__(self, error: ImportError):
        self._error = error

    def find_spec(self, fullname, path=None, target=None):
        if fullname == "vllm" or fullname.startswith("vllm."):
            raise self._error
        return None


class TestVLLMBackend:
    def test_wrapper_classes_inherit_from_vllm(self):
        from ray.llm._internal.serve.core.configs.anthropic_api_models import (
            AnthropicCountTokensRequest,
            AnthropicMessagesRequest,
            AnthropicMessagesResponse,
        )

        assert "vllm" in AnthropicMessagesRequest.__mro__[1].__module__
        assert "vllm" in AnthropicMessagesResponse.__mro__[1].__module__
        assert "vllm" in AnthropicCountTokensRequest.__mro__[1].__module__

    def test_messages_request_round_trip(self):
        from ray.llm._internal.serve.core.configs.anthropic_api_models import (
            AnthropicMessagesRequest,
        )

        request = AnthropicMessagesRequest(
            model="test-model",
            max_tokens=16,
            messages=[{"role": "user", "content": "hello"}],
        )
        dumped = request.model_dump()
        assert dumped["model"] == "test-model"
        assert dumped["messages"][0]["content"] == "hello"

    def _reload_anthropic_models_with_blocker(self, blocker):
        saved = {
            k: sys.modules.pop(k)
            for k in list(sys.modules)
            if k == "vllm" or k.startswith("vllm.")
        }
        sys.modules.pop(_ANTHROPIC_MODELS_MOD, None)
        sys.meta_path.insert(0, blocker)
        try:
            importlib.import_module(_ANTHROPIC_MODELS_MOD)
        finally:
            sys.meta_path.remove(blocker)
            sys.modules.pop(_ANTHROPIC_MODELS_MOD, None)
            sys.modules.update(saved)

    def test_import_error_when_vllm_blocked(self):
        with pytest.raises(ImportError, match="vLLM is not installed"):
            self._reload_anthropic_models_with_blocker(_VLLMImportBlocker())

    def test_vllm_installed_but_broken_cuda(self):
        cuda_err = ImportError(
            "libcudart.so.12: cannot open shared object file: No such file or directory"
        )
        blocker = _VLLMBrokenInstallBlocker(cuda_err)
        with pytest.raises(ImportError, match="vLLM is installed but failed to import"):
            self._reload_anthropic_models_with_blocker(blocker)

    def test_public_reexports_stable(self):
        from ray.serve.llm.anthropic_api_models import AnthropicMessagesRequest

        assert any(
            "vllm" in cls.__module__ for cls in AnthropicMessagesRequest.__mro__[1:]
        )

    def test_build_anthropic_app_in_all(self):
        from ray.serve import llm

        assert "build_anthropic_app" in llm.__all__


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
