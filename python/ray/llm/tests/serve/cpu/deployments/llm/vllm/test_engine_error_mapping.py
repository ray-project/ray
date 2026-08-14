"""Tests that a bad request reaching the engine is still reported as a 4xx.

``AsyncLLM.generate`` only re-raises ``VLLMClientError`` as-is; everything else comes
back wrapped in ``EngineGenerateError``. Some of vLLM's own request validators still
raise a plain ``ValueError`` -- the xgrammar and guidance grammar checks reached from
``SamplingParams.verify`` do, so an unparseable ``response_format`` schema ends up
there -- and reporting those as a 500 would hide a bad request.

Both serving paths are covered: ``_make_error_response`` answers normal requests, while
the direct-streaming app is served by vLLM's own exception handlers.
"""

import sys
import types

import pytest
from starlette.datastructures import State
from vllm.entrypoints.serve.utils.error_response import create_error_response
from vllm.exceptions import VLLMValidationError
from vllm.v1.engine.exceptions import EngineDeadError, EngineGenerateError

from ray.llm._internal.serve.engines.vllm.vllm_engine import (
    VLLMEngine,
    _unwrapping_vllm_error_handler,
)


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
    # vLLM's engine error path reads app.state.server to stop uvicorn; only vLLM's
    # own launcher sets it, and Ray does not run that launcher.
    state.server = types.SimpleNamespace(should_exit=False)
    return types.SimpleNamespace(app=types.SimpleNamespace(state=state), state=State())


# The invalid-schema case: xgrammar rejects the schema, then the auto-backend
# fallback's guidance check raises a plain ValueError.
_GRAMMAR_ERROR = ValueError('Grammar error: unsupported type "str"')


@pytest.mark.parametrize(
    "exc",
    [
        _wrap(_GRAMMAR_ERROR),
        _wrap(VLLMValidationError("bad parameter", parameter="p")),
        VLLMValidationError("bad parameter", parameter="p"),
    ],
)
def test_make_error_response_reports_bad_requests_as_400(exc):
    assert VLLMEngine._make_error_response(_FakeServing, exc).error.code == 400


@pytest.mark.parametrize(
    "exc",
    [
        _wrap(RuntimeError("CUDA error")),
        _wrap(EngineDeadError()),
        EngineGenerateError(),  # no cause at all
    ],
)
def test_make_error_response_propagates_real_engine_failures(exc):
    """Engine faults must keep propagating so Serve still sees them as failures."""
    with pytest.raises(EngineGenerateError):
        VLLMEngine._make_error_response(_FakeServing, exc)


@pytest.mark.parametrize(
    "exc,expected_code",
    [
        (_wrap(_GRAMMAR_ERROR), 400),
        (_wrap(VLLMValidationError("bad parameter", parameter="p")), 400),
        (VLLMValidationError("bad parameter", parameter="p"), 400),
        (_wrap(RuntimeError("CUDA error")), 500),
        (EngineDeadError(), 500),
    ],
)
@pytest.mark.asyncio
async def test_direct_streaming_handler_status_codes(exc, expected_code):
    response = await _unwrapping_vllm_error_handler(_fake_request(), exc)
    assert response.status_code == expected_code


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
