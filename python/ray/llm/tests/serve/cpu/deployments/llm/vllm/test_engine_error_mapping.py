"""A bad request that reaches the engine must still be reported as a 4xx.

``AsyncLLM.generate`` re-raises ``VLLMClientError`` as-is and wraps everything else
in ``EngineGenerateError``, so validators that still raise a plain ``ValueError``
would surface as 500s. Covers both serving paths: ``_make_error_response`` and the
direct-streaming app, which vLLM's own handlers serve.
"""

import sys
import types

import pytest
from starlette.datastructures import State
from vllm.entrypoints.serve.utils.error_response import create_error_response
from vllm.exceptions import VLLMValidationError
from vllm.v1.engine.exceptions import EngineGenerateError

from ray.llm._internal.serve.engines.vllm.vllm_engine import (
    VLLMEngine,
    _unwrapping_vllm_error_handler,
)

# xgrammar rejects the schema, then the auto-backend fallback's guidance check
# raises a plain ValueError.
_GRAMMAR_ERROR = ValueError('Grammar error: unsupported type "str"')


class _FakeServing:
    """Stands in for a vLLM serving object, which delegates to the same helper."""

    create_error_response = staticmethod(create_error_response)


def _wrap(cause: BaseException) -> EngineGenerateError:
    """Build the exception generate() would raise for ``cause``."""
    exc = EngineGenerateError()
    exc.__cause__ = cause
    return exc


def _fake_request():
    """A request carrying the app state vLLM's error handlers read."""
    state = State()
    state.args = types.SimpleNamespace(log_error_stack=False)
    state.engine_client = types.SimpleNamespace(errored=False, is_running=True)
    # Only vLLM's own launcher sets state.server, and Ray does not run it.
    state.server = types.SimpleNamespace(should_exit=False)
    return types.SimpleNamespace(app=types.SimpleNamespace(state=state), state=State())


@pytest.mark.parametrize(
    "exc",
    [_wrap(_GRAMMAR_ERROR), _wrap(VLLMValidationError("bad", parameter="p"))],
)
def test_bad_request_is_400(exc):
    assert VLLMEngine._make_error_response(_FakeServing, exc).error.code == 400


@pytest.mark.parametrize(
    "exc",
    [_wrap(RuntimeError("CUDA error")), EngineGenerateError()],  # cause, then none
)
def test_engine_failure_propagates(exc):
    """Engine faults must keep propagating so Serve still sees them as failures."""
    with pytest.raises(EngineGenerateError):
        VLLMEngine._make_error_response(_FakeServing, exc)


@pytest.mark.parametrize(
    "exc,expected_code",
    [(_wrap(_GRAMMAR_ERROR), 400), (_wrap(RuntimeError("CUDA error")), 500)],
)
@pytest.mark.asyncio
async def test_direct_streaming_status_codes(exc, expected_code):
    response = await _unwrapping_vllm_error_handler(_fake_request(), exc)
    assert response.status_code == expected_code


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
