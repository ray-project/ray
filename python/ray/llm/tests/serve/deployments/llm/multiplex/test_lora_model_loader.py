import asyncio
import subprocess
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
    LoraMirrorConfig,
    ModelLoadingConfig,
)


@pytest.mark.asyncio
async def test_lora_model_loader():
    model_loader = LoraModelLoader("/tmp/ray/lora/cache")
    mock_sync_model = Mock()

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llm_model_id",
        ),
        llm_engine=LLMEngine.VLLM,
        accelerator_type="L4",
        lora_config=LoraConfig(dynamic_lora_loading_path="s3://fake-bucket-uri-abcd"),
    )
    lora_model_id = "base_model:lora_id"
    with patch.multiple(
        "ray.llm._internal.serve.deployments.llm.multiplex.lora_model_loader",
        sync_model=mock_sync_model,
        get_lora_mirror_config=AsyncMock(
            return_value=LoraMirrorConfig(
                lora_model_id=lora_model_id,
                bucket_uri="s3://fake-bucket-uri-abcd",
                max_total_tokens=4096,
            )
        ),
    ):
        disk_multiplex_config = await model_loader.load_model(
            lora_model_id=lora_model_id,
            llm_config=llm_config,
        )

        mock_sync_model.assert_called_once_with(
            "s3://fake-bucket-uri-abcd",
            "/tmp/ray/lora/cache/lora_id",
            timeout=model_loader.download_timeout_s,
            sync_args=None,
        )
        mock_sync_model.reset_mock()

        # Second time we don't load from s3
        new_disk_config = await model_loader.load_model(
            lora_model_id=lora_model_id,
            llm_config=llm_config,
        )
        assert new_disk_config == disk_multiplex_config
        mock_sync_model.assert_not_called()


@pytest.mark.asyncio
async def test_lora_model_loader_task():
    model_id = "lora"

    class MockLoraModelLoader(LoraModelLoader):
        def __init__(self, *args, **kwargs):
            self.task_call_counter = 0
            super().__init__(*args, **kwargs)

        def _load_model_sync(self, *args, **kwargs):
            self.task_call_counter += 1
            return super()._load_model_sync(*args, **kwargs)

    model_loader = MockLoraModelLoader("/tmp/ray/lora/cache", download_timeout_s=1)
    tasks = []

    def _get_aws_sync_command(*args, **kwargs):
        return ["sleep", "100000000"]

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llm_model_id",
        ),
        llm_engine=LLMEngine.VLLM,
        accelerator_type="L4",
        lora_config=LoraConfig(dynamic_lora_loading_path="s3://fake-bucket-uri-abcd"),
    )
    with patch(
        "ray.llm._internal.serve.deployments.llm.multiplex.utils._get_aws_sync_command",
        _get_aws_sync_command,
    ), patch.multiple(
        "ray.llm._internal.serve.deployments.llm.multiplex.lora_model_loader",
        get_lora_mirror_config=AsyncMock(
            return_value=LoraMirrorConfig(
                lora_model_id="lora",
                bucket_uri="s3://fake-bucket-uri-abcd",
                max_total_tokens=4096,
            )
        ),
    ):
        tasks.append(
            asyncio.create_task(
                model_loader.load_model(
                    lora_model_id=model_id,
                    llm_config=llm_config,
                )
            )
        )
        await asyncio.sleep(0.1)
        tasks.append(
            asyncio.create_task(
                model_loader.load_model(
                    lora_model_id=model_id,
                    llm_config=llm_config,
                )
            )
        )
        await asyncio.sleep(0.7)
        tasks.append(
            asyncio.create_task(
                model_loader.load_model(
                    lora_model_id=model_id,
                    llm_config=llm_config,
                )
            )
        )

        # Ensure that the rest of the tasks finish even if the original one
        # is cancelled
        tasks.pop(0).cancel()
        with pytest.raises(subprocess.TimeoutExpired):
            await asyncio.gather(*tasks)

        assert all(isinstance(t.exception(), subprocess.TimeoutExpired) for t in tasks)
        # The download task should have been called only once across all requests
        assert model_loader.task_call_counter == 1

    tasks.clear()

    def _get_aws_sync_command(*args, **kwargs):
        return ["sleep", "0.001"]

    with patch(
        "ray.llm._internal.serve.deployments.llm.multiplex.utils._get_aws_sync_command",
        _get_aws_sync_command,
    ), patch.multiple(
        "ray.llm._internal.serve.deployments.llm.multiplex.lora_model_loader",
        get_lora_mirror_config=AsyncMock(
            return_value=LoraMirrorConfig(
                lora_model_id="lora",
                bucket_uri="s3://fake-bucket-uri-abcd",
                max_total_tokens=4096,
            )
        ),
    ):
        tasks.append(
            asyncio.create_task(
                model_loader.load_model(
                    lora_model_id=model_id,
                    llm_config=llm_config,
                )
            )
        )
        await asyncio.sleep(0)
        tasks.append(
            asyncio.create_task(
                model_loader.load_model(
                    lora_model_id=model_id,
                    llm_config=llm_config,
                )
            )
        )
        await asyncio.gather(*tasks)
        assert all(not t.exception() for t in tasks)
        # The download task should have been called only once across all requests
        assert model_loader.task_call_counter == 2
        assert model_id in model_loader.disk_cache
        assert not model_loader.active_syncing_tasks

        tasks.clear()

        tasks.append(
            asyncio.create_task(
                model_loader.load_model(
                    lora_model_id=model_id,
                    llm_config=llm_config,
                )
            )
        )
        await asyncio.sleep(0)
        tasks.append(
            asyncio.create_task(
                model_loader.load_model(
                    lora_model_id=model_id,
                    llm_config=llm_config,
                )
            )
        )
        await asyncio.gather(*tasks)
        assert all(not t.exception() for t in tasks)
        # We should have loaded from cache here
        assert model_loader.task_call_counter == 2


@pytest.mark.asyncio
async def test_lora_model_loader_task_retry():
    model_id = "lora"

    class MockLoraModelLoader(LoraModelLoader):
        def __init__(self, *args, **kwargs):
            self.task_call_counter = 0
            super().__init__(*args, **kwargs)

        def _load_model_sync(self, *args, **kwargs):
            self.task_call_counter += 1
            return super()._load_model_sync(*args, **kwargs)

    model_loader = MockLoraModelLoader(
        "/tmp/ray/lora/cache", download_timeout_s=1, max_tries=3
    )
    tasks = []
    tries = 0

    def _get_aws_sync_command(*args, **kwargs):
        nonlocal tries
        tries += 1
        if tries > 1:
            return ["sleep", "0.1"]
        return ["sleep", "100000000"]

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="llm_model_id",
        ),
        llm_engine=LLMEngine.VLLM,
        accelerator_type="L4",
        lora_config=LoraConfig(dynamic_lora_loading_path="s3://fake-bucket-uri-abcd"),
    )
    with patch(
        "ray.llm._internal.serve.deployments.llm.multiplex.utils._get_aws_sync_command",
        _get_aws_sync_command,
    ), patch.multiple(
        "ray.llm._internal.serve.deployments.llm.multiplex.lora_model_loader",
        get_lora_mirror_config=AsyncMock(
            return_value=LoraMirrorConfig(
                lora_model_id="lora",
                bucket_uri="s3://fake-bucket-uri-abcd",
                max_total_tokens=4096,
            )
        ),
    ):
        tasks.append(
            asyncio.create_task(
                model_loader.load_model(
                    lora_model_id=model_id,
                    llm_config=llm_config,
                )
            )
        )
        await asyncio.sleep(0.1)
        tasks.append(
            asyncio.create_task(
                model_loader.load_model(
                    lora_model_id=model_id,
                    llm_config=llm_config,
                )
            )
        )
        await asyncio.sleep(0.7)
        tasks.append(
            asyncio.create_task(
                model_loader.load_model(
                    lora_model_id=model_id,
                    llm_config=llm_config,
                )
            )
        )
        await asyncio.gather(*tasks)

        assert all(not t.exception() for t in tasks)
        # The download task should have been called only once across all requests
        assert model_loader.task_call_counter == 1
        assert tries == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
