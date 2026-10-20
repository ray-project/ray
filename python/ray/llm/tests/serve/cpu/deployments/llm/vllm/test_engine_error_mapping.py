"""A bad request that reaches the engine must still be reported as a 4xx.

``AsyncLLM.generate`` re-raises ``VLLMClientError`` as-is and wraps everything else
in ``EngineGenerateError``. ``VLLMClientError`` is not a ``ValueError``, so the
serving path has to catch it by name, and ``EngineGenerateError`` has to stay
uncaught so Serve still sees a failure.
"""

import sys

import pytest
from vllm.entrypoints.serve.exception_handling.error_response import (
    create_error_response,
)
from vllm.exceptions import VLLMValidationError
from vllm.v1.engine.exceptions import EngineGenerateError

from ray.llm._internal.serve.core.configs.openai_api_models import ChatCompletionRequest
from ray.llm._internal.serve.engines.vllm.vllm_engine import VLLMEngine

# vLLM still raises plain ValueErrors from the paths not yet migrated to
# VLLMError, so both kinds have to map to a 4xx.
_GRAMMAR_ERROR = ValueError('Grammar error: unsupported type "str"')


class _FakeServing:
    """Stands in for a vLLM serving object, which delegates to the same helper."""

    create_error_response = staticmethod(create_error_response)

    def __init__(self, exc: BaseException):
        self._exc = exc

    async def create_chat_completion(self, request, raw_request=None):
        raise self._exc


def _chat(exc):
    """Run chat() against a serving object that raises ``exc``."""
    engine = VLLMEngine.__new__(VLLMEngine)
    engine._oai_serving_chat = _FakeServing(exc)
    return engine.chat(
        ChatCompletionRequest(model="m", messages=[{"role": "user", "content": "x"}])
    )


@pytest.mark.parametrize(
    "exc", [_GRAMMAR_ERROR, VLLMValidationError("bad", parameter="p")]
)
@pytest.mark.asyncio
async def test_bad_request_is_400(exc):
    assert [response.error.code async for response in _chat(exc)] == [400]


@pytest.mark.asyncio
async def test_engine_failure_propagates():
    """Engine faults must keep propagating so Serve still sees them as failures."""
    with pytest.raises(EngineGenerateError):
        async for _ in _chat(EngineGenerateError()):
            pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
