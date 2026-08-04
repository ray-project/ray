import asyncio

import pytest

from ray.llm._internal.common.callbacks.base import (
    CallbackBase,
)
from ray.llm._internal.common.utils.download_utils import NodeModelDownloadable
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)


class TestingCallback(CallbackBase):
    def __init__(self, llm_config, raise_error_on_callback: bool = True, **kwargs):
        super().__init__(llm_config, raise_error_on_callback, **kwargs)
        self.before_init_called = False
        self.after_init_called = False
        self.before_init_ctx = None
        self.after_init_ctx = None
        assert kwargs["kwargs_test_key"] == "kwargs_test_value"

    async def on_before_node_init(self) -> None:
        assert (
            self.ctx.worker_node_download_model
            == NodeModelDownloadable.MODEL_AND_TOKENIZER
        )
        self.ctx.worker_node_download_model = NodeModelDownloadable.NONE

        self.ctx.custom_data["ctx_test_key"] = "ctx_test_value"
        self.before_init_called = True
        self.ctx.run_init_node = False

    async def on_after_node_init(self) -> None:
        assert self.ctx.worker_node_download_model == NodeModelDownloadable.NONE

        self.after_init_called = True
        assert self.ctx.custom_data["ctx_test_key"] == "ctx_test_value"


class TestCallbackBase:
    @pytest.fixture
    def llm_config(self):
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            llm_engine="vLLM",
            callback_config={
                "callback_class": TestingCallback,
                "callback_kwargs": {"kwargs_test_key": "kwargs_test_value"},
            },
        )
        return config

    def test_callback_methods_called(self, llm_config):
        """Test that callback methods are called during initialization."""

        # Run initialization
        async def run_initialization():
            callback = llm_config.get_or_create_callback()
            await callback.run_callback("on_before_node_init")
            if callback.ctx.run_init_node:
                raise Exception("run_init_node is True")
            await callback.run_callback("on_after_node_init")

        asyncio.run(run_initialization())
        # Verify callback was created and methods were called

        callback = llm_config.get_or_create_callback()
        assert callback is not None
        assert isinstance(callback, TestingCallback)
        assert callback.before_init_called is True
        assert callback.after_init_called is True

    def test_callback_singleton_behavior(self, llm_config):
        """Test that callback instance is cached (singleton pattern)."""
        # Get callback multiple times
        callback1 = llm_config.get_or_create_callback()
        callback2 = llm_config.get_or_create_callback()

        # Should be the same instance
        assert callback1 is callback2

    def test_callback_must_inherit_from_callback_class(self):
        """Test that callback_class must be a subclass of Callback, not just implement the same methods."""

        class FakeCallback:
            """A class that implements the same methods as Callback but doesn't inherit from it."""

            def __init__(self, **kwargs):
                pass

            async def on_before_node_init(self):
                pass

            async def on_after_node_init(self):
                pass

        # Should raise an error when trying to create callback
        with pytest.raises(Exception, match="is-subclass"):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test-model"),
                llm_engine="vLLM",
                callback_config={
                    "callback_class": FakeCallback,
                    "callback_kwargs": {},
                },
            )


if __name__ == "__main__":
    pytest.main(["-v", __file__])
