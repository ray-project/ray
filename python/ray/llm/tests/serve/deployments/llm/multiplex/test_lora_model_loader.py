import asyncio
from unittest.mock import AsyncMock, Mock, patch
import sys

import pytest


from ray.llm._internal.serve.deployments.llm.multiplex.lora_model_loader import (
    LoraModelLoader,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    LLMEngine,
    LoraConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.common.utils.cloud_utils import LoraMirrorConfig


class TestLoRAModelLoader:
    """Test suite for the LoraModelLoader class."""

    @pytest.fixture
    def model_loader(self):
        """Provides a LoraModelLoader instance for tests."""
        return LoraModelLoader("/tmp/ray/lora/cache", max_tries=3)

    @pytest.fixture
    def llm_config(self):
        """Common LLM config used across tests."""
        return LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="llm_model_id"),
            llm_engine=LLMEngine.vLLM,
            accelerator_type="L4",
            lora_config=LoraConfig(
                dynamic_lora_loading_path="s3://fake-bucket-uri-abcd"
            ),
        )

    @pytest.fixture
    def lora_model_id(self):
        """Common LoRA model ID used across tests."""
        return "base_model:lora_id"

    @pytest.fixture
    def lora_mirror_config(self, lora_model_id):
        """Common LoRA mirror config used across tests."""
        return LoraMirrorConfig(
            lora_model_id=lora_model_id,
            bucket_uri="s3://fake-bucket-uri-abcd",
            max_total_tokens=4096,
        )

    @pytest.mark.asyncio
    async def test_basic_loading(
        self, model_loader, llm_config, lora_model_id, lora_mirror_config
    ):
        """Test basic model loading functionality."""
        # Create a simple mock for sync_model
        mock_sync_model = Mock()

        with patch.multiple(
            "ray.llm._internal.serve.deployments.llm.multiplex.lora_model_loader",
            sync_model=mock_sync_model,
            get_lora_mirror_config=AsyncMock(return_value=lora_mirror_config),
        ):
            # First load should download the model
            disk_multiplex_config = await model_loader.load_model(
                lora_model_id=lora_model_id,
                llm_config=llm_config,
            )

            # Verify sync_model was called with correct parameters
            mock_sync_model.assert_called_once_with(
                "s3://fake-bucket-uri-abcd",
                "/tmp/ray/lora/cache/lora_id",
                timeout=model_loader.download_timeout_s,
                sync_args=None,
            )
            mock_sync_model.reset_mock()

            # Second time we don't load from S3 - should use cache
            new_disk_config = await model_loader.load_model(
                lora_model_id=lora_model_id,
                llm_config=llm_config,
            )
            assert new_disk_config == disk_multiplex_config
            mock_sync_model.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_logic(
        self, model_loader, llm_config, lora_model_id, lora_mirror_config
    ):
        """Test that the lora model load task is properly retried on failure."""
        # Counter to track number of sync_model calls
        attempt_count = 0

        # Create a mock for sync_model that tracks calls and fails initially
        def mock_sync_model(bucket_uri, local_path, timeout=None, sync_args=None):
            nonlocal attempt_count
            attempt_count += 1

            # Fail on first attempt, succeed on second
            if attempt_count == 1:
                raise RuntimeError("Simulated download failure")
            # Success on subsequent attempts
            return None

        with patch.multiple(
            "ray.llm._internal.serve.deployments.llm.multiplex.lora_model_loader",
            sync_model=Mock(side_effect=mock_sync_model),
            get_lora_mirror_config=AsyncMock(return_value=lora_mirror_config),
        ):
            # First load should trigger a retry
            disk_multiplex_config = await model_loader.load_model(
                lora_model_id=lora_model_id,
                llm_config=llm_config,
            )

            # Verify retry happened exactly once
            assert attempt_count == 2

            # Reset counter
            attempt_count = 0

            # Load again (should use cache, no download attempts)
            new_disk_config = await model_loader.load_model(
                lora_model_id=lora_model_id,
                llm_config=llm_config,
            )

            # Verify no new download attempts
            assert attempt_count == 0

            # Verify cached config is returned
            assert new_disk_config == disk_multiplex_config

    @pytest.mark.asyncio
    async def test_concurrent_loading(
        self, model_loader, llm_config, lora_model_id, lora_mirror_config
    ):
        """Test that concurrent loads only trigger one download process."""
        # Counter to track number of sync_model calls
        attempt_count = 0

        # Create a mock for sync_model that tracks calls and fails initially
        def mock_sync_model(bucket_uri, local_path, timeout=None, sync_args=None):
            nonlocal attempt_count
            attempt_count += 1

            # Fail on first attempt, succeed on second
            if attempt_count == 1:
                raise RuntimeError("Simulated download failure")
            # Success on subsequent attempts
            return None

        with patch.multiple(
            "ray.llm._internal.serve.deployments.llm.multiplex.lora_model_loader",
            sync_model=Mock(side_effect=mock_sync_model),
            get_lora_mirror_config=AsyncMock(return_value=lora_mirror_config),
        ):
            # Clear cache to force download
            model_loader.disk_cache.clear()

            # Create multiple concurrent tasks
            tasks = [
                asyncio.create_task(
                    model_loader.load_model(
                        lora_model_id=lora_model_id,
                        llm_config=llm_config,
                    )
                )
                for _ in range(3)
            ]

            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks)

            # Verify retry happened exactly once across all tasks
            assert attempt_count == 2

            # All tasks should return the same result
            assert all(result == results[0] for result in results)

    @pytest.mark.asyncio
    async def test_max_retries_exhaustion(
        self, model_loader, llm_config, lora_model_id, lora_mirror_config
    ):
        """Test that an error is raised when max retries are exhausted."""
        # Mock that always fails
        def mock_sync_model_always_fails(*args, **kwargs):
            raise RuntimeError("Simulated persistent failure")

        with patch.multiple(
            "ray.llm._internal.serve.deployments.llm.multiplex.lora_model_loader",
            sync_model=Mock(side_effect=mock_sync_model_always_fails),
            get_lora_mirror_config=AsyncMock(return_value=lora_mirror_config),
        ):
            # Should fail after max_tries (3) attempts
            with pytest.raises(RuntimeError) as excinfo:
                await model_loader.load_model(
                    lora_model_id=lora_model_id,
                    llm_config=llm_config,
                )

            assert "Simulated persistent failure" in str(excinfo.value)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
