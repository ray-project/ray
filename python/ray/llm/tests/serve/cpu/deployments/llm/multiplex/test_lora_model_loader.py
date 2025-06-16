import asyncio
import sys
from unittest.mock import Mock, patch

import pytest

from ray.llm._internal.common.utils.cloud_utils import LoraMirrorConfig
from ray.llm._internal.common.utils.download_utils import (
    LoraModelLoader,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    LLMEngine,
    LoraConfig,
    ModelLoadingConfig,
)


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
    async def test_basic_loading(self, model_loader, lora_model_id, lora_mirror_config):
        """Test basic model loading functionality."""
        # Create a simple mock for CloudFileSystem.download_files
        mock_download_files = Mock()

        with patch.multiple(
            "ray.llm._internal.common.utils.download_utils",
            CloudFileSystem=Mock(download_files=mock_download_files),
        ):
            # First load should download the model
            disk_multiplex_config = await model_loader.load_model(
                lora_model_id=lora_model_id,
                lora_mirror_config=lora_mirror_config,
            )

            # Verify download_files was called with correct parameters
            mock_download_files.assert_called_once_with(
                path="/tmp/ray/lora/cache/lora_id",
                bucket_uri="s3://fake-bucket-uri-abcd",
            )
            mock_download_files.reset_mock()

            # Second time we don't load from S3 - should use cache
            new_disk_config = await model_loader.load_model(
                lora_model_id=lora_model_id,
                lora_mirror_config=lora_mirror_config,
            )
            assert new_disk_config == disk_multiplex_config
            mock_download_files.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_logic(self, model_loader, lora_model_id, lora_mirror_config):
        """Test that the lora model load task is properly retried on failure."""
        # Counter to track number of download_files calls
        attempt_count = 0

        # Create a mock for download_files that tracks calls and fails initially
        def mock_download_files(path, bucket_uri):
            nonlocal attempt_count
            attempt_count += 1

            # Fail on first attempt, succeed on second
            if attempt_count == 1:
                raise RuntimeError("Simulated download failure")
            # Success on subsequent attempts
            return None

        with patch.multiple(
            "ray.llm._internal.common.utils.download_utils",
            CloudFileSystem=Mock(download_files=Mock(side_effect=mock_download_files)),
        ):
            # First load should trigger a retry
            disk_multiplex_config = await model_loader.load_model(
                lora_model_id=lora_model_id,
                lora_mirror_config=lora_mirror_config,
            )

            # Verify retry happened exactly once
            assert attempt_count == 2

            # Reset counter
            attempt_count = 0

            # Load again (should use cache, no download attempts)
            new_disk_config = await model_loader.load_model(
                lora_model_id=lora_model_id,
                lora_mirror_config=lora_mirror_config,
            )

            # Verify no new download attempts
            assert attempt_count == 0

            # Verify cached config is returned
            assert new_disk_config == disk_multiplex_config

    @pytest.mark.asyncio
    async def test_concurrent_loading(
        self, model_loader, lora_model_id, lora_mirror_config
    ):
        """Test that concurrent loads only trigger one download process."""
        # Counter to track number of download_files calls
        attempt_count = 0

        # Create a mock for download_files that tracks calls and fails initially
        def mock_download_files(path, bucket_uri):
            nonlocal attempt_count
            attempt_count += 1

            # Fail on first attempt, succeed on second
            if attempt_count == 1:
                raise RuntimeError("Simulated download failure")
            # Success on subsequent attempts
            return None

        with patch.multiple(
            "ray.llm._internal.common.utils.download_utils",
            CloudFileSystem=Mock(download_files=Mock(side_effect=mock_download_files)),
        ):
            # Clear cache to force download
            model_loader.disk_cache.clear()

            # Create multiple concurrent tasks
            tasks = [
                asyncio.create_task(
                    model_loader.load_model(
                        lora_model_id=lora_model_id,
                        lora_mirror_config=lora_mirror_config,
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
        self, model_loader, lora_model_id, lora_mirror_config
    ):
        """Test that an error is raised when max retries are exhausted."""
        # Mock that always fails
        def mock_download_files_always_fails(*args, **kwargs):
            raise RuntimeError("Simulated persistent failure")

        with patch.multiple(
            "ray.llm._internal.common.utils.download_utils",
            CloudFileSystem=Mock(
                download_files=Mock(side_effect=mock_download_files_always_fails)
            ),
        ):
            # Should fail after max_tries (3) attempts
            with pytest.raises(RuntimeError) as excinfo:
                await model_loader.load_model(
                    lora_model_id=lora_model_id,
                    lora_mirror_config=lora_mirror_config,
                )

            assert "Simulated persistent failure" in str(excinfo.value)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
