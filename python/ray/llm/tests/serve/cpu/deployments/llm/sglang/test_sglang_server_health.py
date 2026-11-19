"""SGLangServer.check_health must surface RayEngine.check_health failures.

Serve replaces a replica only when its check_health raises. RayEngine reports a
dead or stuck scheduler by raising from its own check_health; SGLang versions
that predate it must keep working.
"""

import sys

import pytest

from ray.llm._internal.serve.engines.sglang.sglang_engine import SGLangServer


class _FakeRayEngine:
    def __init__(self, error=None):
        self.error = error
        self.calls = 0

    async def check_health(self):
        self.calls += 1
        if self.error is not None:
            raise self.error


def _server(engine) -> SGLangServer:
    server = SGLangServer.__new__(SGLangServer)
    server.engine = engine
    return server


async def test_engine_failure_fails_replica_health_check():
    engine = _FakeRayEngine(RuntimeError("SGLang scheduler actor died"))
    with pytest.raises(RuntimeError, match="scheduler actor died"):
        await _server(engine).check_health()
    assert engine.calls == 1


async def test_healthy_engine_passes():
    engine = _FakeRayEngine()
    await _server(engine).check_health()
    assert engine.calls == 1


async def test_engine_without_check_health_passes():
    """SGLang releases without RayEngine.check_health keep the old no-op."""
    await _server(object()).check_health()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
