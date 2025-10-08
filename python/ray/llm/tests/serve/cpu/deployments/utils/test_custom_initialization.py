import asyncio
from unittest.mock import MagicMock, patch

import pytest

from ray.llm._internal.common.utils.download_utils import NodeModelDownloadable
from ray.llm._internal.serve.callbacks.custom_initialization import (
    Callback,
    CallbackCtx,
)
from ray.llm._internal.serve.configs.server_models import LLMConfig, ModelLoadingConfig
from ray.llm._internal.serve.deployments.utils.node_initialization_utils import (
    initialize_node,
)


class TestingCallback(Callback):
    def __init__(self, **kwargs):
        self.before_init_called = False
        self.after_init_called = False
        self.before_init_ctx = None
        self.after_init_ctx = None
        assert "test_key" in kwargs and kwargs["test_key"] == "test_value"

    async def on_before_node_init(self, ctx: CallbackCtx) -> None:
        assert ctx.local_node_download_model == NodeModelDownloadable.TOKENIZER_ONLY
        assert (
            ctx.worker_node_download_model == NodeModelDownloadable.MODEL_AND_TOKENIZER
        )

        ctx.local_node_download_model = NodeModelDownloadable.NONE
        ctx.worker_node_download_model = NodeModelDownloadable.NONE

        ctx.custom_data["test_key"] = "test_value"
        self.before_init_called = True

    async def on_after_node_init(self, ctx: CallbackCtx) -> None:
        assert ctx.local_node_download_model == NodeModelDownloadable.NONE
        assert ctx.worker_node_download_model == NodeModelDownloadable.NONE

        self.after_init_called = True
        assert ctx.custom_data["test_key"] == "test_value"


class TestCustomInitialization:
    @pytest.fixture
    def llm_config(self):
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            llm_engine="vLLM",
            callback_config={
                "callback_class": "test_custom_initialization.TestingCallback",
                "callback_kwargs": {"test_key": "test_value"},
            },
        )
        return config

    @patch(
        "ray.llm._internal.serve.deployments.utils.node_initialization_utils.download_model_files"
    )
    @patch("vllm.transformers_utils.tokenizer.get_tokenizer")
    @patch(
        "ray.llm._internal.serve.deployments.utils.node_initialization_utils.transformers"
    )
    def test_callback_methods_called(
        self,
        mock_transformers,
        mock_get_tokenizer,
        mock_download_model_files,
        llm_config,
    ):
        """Test that callback methods are called during initialization."""
        # Setup mocks for external dependencies
        mock_download_model_files.return_value = None
        mock_get_tokenizer.return_value = MagicMock()
        mock_transformers.AutoTokenizer.from_pretrained.return_value = MagicMock()

        # Run initialization
        asyncio.run(initialize_node(llm_config))

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

            async def on_before_node_init(self, ctx):
                pass

            async def on_after_node_init(self, ctx):
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
