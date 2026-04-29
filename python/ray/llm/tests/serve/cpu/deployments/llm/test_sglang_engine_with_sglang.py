"""Tests for SGLangServer lifecycle methods using the real SGLang package.

Skipped automatically when sglang is not installed. No GPU required — sglang.Engine
is mocked so no model is loaded, but all imports and protocol types are real. This
catches bugs that the fully-mocked test_sglang_engine.py cannot, such as the
openai_api_models import chain resolving correctly against the installed sglang version.

Run with: pytest python/ray/llm/tests/serve/cpu/deployments/llm/test_sglang_engine_with_sglang.py -v
"""

import sys

import pytest

# Auto-skips the entire module if sglang is not installed.
# Must come before any ray.llm imports so that openai_api_models can import
# real sglang protocol types rather than hitting an ImportError.
sglang = pytest.importorskip("sglang", reason="sglang is not installed")

from unittest.mock import AsyncMock, MagicMock, patch  # noqa: E402

from pydantic import ValidationError  # noqa: E402

from ray.llm._internal.serve.core.configs.llm_config import (  # noqa: E402
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.engines.sglang.sglang_engine import (  # noqa: E402
    SGLangPauseConfig,
    SGLangServer,
    SGLangSleepConfig,
    SGLangWakeupConfig,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_llm_config() -> LLMConfig:
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="mock-sglang-model"),
        runtime_env={},
        log_engine_metrics=False,
    )


def _make_server(mock_engine: AsyncMock) -> SGLangServer:
    """Construct SGLangServer with a real sglang import but a mocked Engine.

    patch("sglang.Engine") cannot be used here because accessing sglang.Engine
    triggers sglang's lazy loader which requires torch. Instead we temporarily
    swap sys.modules['sglang'] with a lightweight mock during __init__ only —
    the real sglang module is already imported at module level so all protocol
    types remain real.
    """
    mock_sglang = MagicMock()
    mock_sglang.Engine.return_value = mock_engine
    with patch.dict(sys.modules, {"sglang": mock_sglang}):
        server = SGLangServer(_make_llm_config())
    server.engine = mock_engine
    return server


@pytest.fixture
def mock_engine() -> AsyncMock:
    engine = AsyncMock()
    engine.pause_generation = AsyncMock()
    engine.continue_generation = AsyncMock()
    engine.release_memory_occupation = AsyncMock()
    engine.resume_memory_occupation = AsyncMock()
    engine.flush_cache = AsyncMock()
    engine.collective_rpc = AsyncMock(return_value=None)
    return engine


@pytest.fixture
def server(mock_engine: AsyncMock) -> SGLangServer:
    return _make_server(mock_engine)


# ---------------------------------------------------------------------------
# Verify import chain with real sglang
# ---------------------------------------------------------------------------

class TestImports:
    def test_sglang_protocol_types_importable(self):
        """openai_api_models must resolve correctly against the installed sglang."""
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            ChatCompletionRequest,
            CompletionRequest,
            EmbeddingRequest,
            TokenizeResponse,
        )
        assert ChatCompletionRequest is not None
        assert CompletionRequest is not None
        assert EmbeddingRequest is not None
        assert TokenizeResponse is not None

    def test_config_models_importable(self):
        assert SGLangPauseConfig is not None
        assert SGLangSleepConfig is not None
        assert SGLangWakeupConfig is not None


# ---------------------------------------------------------------------------
# Verify SGLangServer.__init__ calls sglang.Engine
# ---------------------------------------------------------------------------

class TestServerInit:
    def test_engine_constructed_with_llm_config_kwargs(self):
        llm_config = _make_llm_config()
        mock_engine = AsyncMock()
        mock_sglang = MagicMock()
        mock_sglang.Engine.return_value = mock_engine
        with patch.dict(sys.modules, {"sglang": mock_sglang}):
            SGLangServer(llm_config)
        mock_sglang.Engine.assert_called_once_with(**llm_config.engine_kwargs)

    @pytest.mark.asyncio
    async def test_initial_is_paused_false(self, server: SGLangServer):
        assert await server.is_paused() is False

    @pytest.mark.asyncio
    async def test_initial_is_sleeping_false(self, server: SGLangServer):
        assert await server.is_sleeping() is False


# ---------------------------------------------------------------------------
# SGLangPauseConfig — with real pydantic + real sglang types in scope
# ---------------------------------------------------------------------------

class TestSGLangPauseConfig:
    def test_default_mode(self):
        assert SGLangPauseConfig().mode == "abort"

    @pytest.mark.parametrize("mode", ["abort", "in_place", "retract"])
    def test_valid_modes(self, mode: str):
        assert SGLangPauseConfig(mode=mode).mode == mode

    def test_invalid_mode_raises(self):
        with pytest.raises(ValidationError):
            SGLangPauseConfig(mode="unknown")


# ---------------------------------------------------------------------------
# SGLangSleepConfig
# ---------------------------------------------------------------------------

class TestSGLangSleepConfig:
    def test_default_tags_is_none(self):
        assert SGLangSleepConfig().tags is None

    @pytest.mark.parametrize("tags", [
        ["kv_cache"],
        ["weights"],
        ["cuda_graph"],
        ["kv_cache", "weights", "cuda_graph"],
    ])
    def test_valid_tags(self, tags):
        assert SGLangSleepConfig(tags=tags).tags == tags

    def test_invalid_tag_raises(self):
        with pytest.raises(ValidationError):
            SGLangSleepConfig(tags=["bad_tag"])


# ---------------------------------------------------------------------------
# SGLangWakeupConfig
# ---------------------------------------------------------------------------

class TestSGLangWakeupConfig:
    def test_default_tags_is_none(self):
        assert SGLangWakeupConfig().tags is None

    @pytest.mark.parametrize("tags", [
        ["kv_cache"],
        ["weights"],
        ["cuda_graph"],
        ["kv_cache", "weights"],
    ])
    def test_valid_tags(self, tags):
        assert SGLangWakeupConfig(tags=tags).tags == tags

    def test_invalid_tag_raises(self):
        with pytest.raises(ValidationError):
            SGLangWakeupConfig(tags=["bad_tag"])


# ---------------------------------------------------------------------------
# pause / resume
# ---------------------------------------------------------------------------

class TestPauseResume:
    @pytest.mark.asyncio
    async def test_pause_sets_is_paused(self, server: SGLangServer, mock_engine: AsyncMock):
        await server.pause()
        assert await server.is_paused() is True

    @pytest.mark.asyncio
    async def test_pause_calls_engine_with_default_mode(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.pause()
        mock_engine.pause_generation.assert_awaited_once_with(mode="abort")

    @pytest.mark.asyncio
    async def test_pause_passes_mode_kwarg(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.pause(mode="retract")
        mock_engine.pause_generation.assert_awaited_once_with(mode="retract")

    @pytest.mark.asyncio
    async def test_pause_rejects_invalid_mode(self, server: SGLangServer):
        with pytest.raises(ValidationError):
            await server.pause(mode="bad_mode")

    @pytest.mark.asyncio
    async def test_resume_clears_is_paused(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.pause()
        await server.resume()
        assert await server.is_paused() is False

    @pytest.mark.asyncio
    async def test_resume_calls_continue_generation(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.resume()
        mock_engine.continue_generation.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_pause_without_engine_raises(self, mock_engine: AsyncMock):
        server = _make_server(mock_engine)
        server.engine = None
        with pytest.raises(AssertionError, match="server is not initialized"):
            await server.pause()

    @pytest.mark.asyncio
    async def test_resume_without_engine_raises(self, mock_engine: AsyncMock):
        server = _make_server(mock_engine)
        server.engine = None
        with pytest.raises(AssertionError, match="server is not initialized"):
            await server.resume()


# ---------------------------------------------------------------------------
# sleep / wakeup
# ---------------------------------------------------------------------------

class TestSleepWakeup:
    @pytest.mark.asyncio
    async def test_sleep_sets_is_sleeping(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.sleep()
        assert await server.is_sleeping() is True

    @pytest.mark.asyncio
    async def test_sleep_calls_release_memory_with_no_tags(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.sleep()
        mock_engine.release_memory_occupation.assert_awaited_once_with(tags=None)

    @pytest.mark.asyncio
    async def test_sleep_passes_tags(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.sleep(tags=["kv_cache", "weights"])
        mock_engine.release_memory_occupation.assert_awaited_once_with(
            tags=["kv_cache", "weights"]
        )

    @pytest.mark.asyncio
    async def test_sleep_rejects_invalid_tags(self, server: SGLangServer):
        with pytest.raises(ValidationError):
            await server.sleep(tags=["bad_tag"])

    @pytest.mark.asyncio
    async def test_wakeup_clears_is_sleeping(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.sleep()
        await server.wakeup()
        assert await server.is_sleeping() is False

    @pytest.mark.asyncio
    async def test_wakeup_calls_resume_memory_with_no_tags(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.wakeup()
        mock_engine.resume_memory_occupation.assert_awaited_once_with(tags=None)

    @pytest.mark.asyncio
    async def test_wakeup_passes_tags(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.wakeup(tags=["cuda_graph"])
        mock_engine.resume_memory_occupation.assert_awaited_once_with(tags=["cuda_graph"])

    @pytest.mark.asyncio
    async def test_wakeup_rejects_invalid_tags(self, server: SGLangServer):
        with pytest.raises(ValidationError):
            await server.wakeup(tags=["bad_tag"])

    @pytest.mark.asyncio
    async def test_sleep_without_engine_raises(self, mock_engine: AsyncMock):
        server = _make_server(mock_engine)
        server.engine = None
        with pytest.raises(AssertionError, match="server is not initialized"):
            await server.sleep()

    @pytest.mark.asyncio
    async def test_wakeup_without_engine_raises(self, mock_engine: AsyncMock):
        server = _make_server(mock_engine)
        server.engine = None
        with pytest.raises(AssertionError, match="server is not initialized"):
            await server.wakeup()


# ---------------------------------------------------------------------------
# reset_prefix_cache
# ---------------------------------------------------------------------------

class TestResetPrefixCache:
    @pytest.mark.asyncio
    async def test_calls_flush_cache_with_timeout(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.reset_prefix_cache(timeout=5.0)
        mock_engine.flush_cache.assert_awaited_once_with(5.0)

    @pytest.mark.asyncio
    async def test_calls_flush_cache_with_none_timeout(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.reset_prefix_cache(timeout=None)
        mock_engine.flush_cache.assert_awaited_once_with(None)

    @pytest.mark.asyncio
    async def test_raises_without_engine(self, mock_engine: AsyncMock):
        server = _make_server(mock_engine)
        server.engine = None
        with pytest.raises(AssertionError, match="server is not initialized"):
            await server.reset_prefix_cache(timeout=None)


# ---------------------------------------------------------------------------
# collective_rpc
# ---------------------------------------------------------------------------

class TestCollectiveRpc:
    @pytest.mark.asyncio
    async def test_returns_none(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        result = await server.collective_rpc("update_weights")
        assert result is None

    @pytest.mark.asyncio
    async def test_passes_method_and_kwargs(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.collective_rpc("sync_weights", kwargs={"scale": 0.5})
        mock_engine.collective_rpc.assert_awaited_once_with(
            method="sync_weights", kwargs={"scale": 0.5}
        )

    @pytest.mark.asyncio
    async def test_none_kwargs_becomes_empty_dict(
        self, server: SGLangServer, mock_engine: AsyncMock
    ):
        await server.collective_rpc("sync_weights", kwargs=None)
        mock_engine.collective_rpc.assert_awaited_once_with(
            method="sync_weights", kwargs={}
        )

    @pytest.mark.asyncio
    async def test_raises_without_engine(self, mock_engine: AsyncMock):
        server = _make_server(mock_engine)
        server.engine = None
        with pytest.raises(AssertionError, match="server is not initialized"):
            await server.collective_rpc("update_weights")
