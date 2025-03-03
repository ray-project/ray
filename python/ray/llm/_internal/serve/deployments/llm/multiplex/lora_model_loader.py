import asyncio
import os
from typing import Dict, Optional

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.deployments.llm.multiplex.utils import (
    clean_model_id,
    clear_directory,
    get_lora_id,
    get_lora_mirror_config,
    sync_model,
    make_async,
    retry_with_exponential_backoff,
)
from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    LLMConfig,
    LoraMirrorConfig,
)

logger = get_logger(__name__)


class GlobalCounter:
    """Manage a global counter

    This counter should be a singleton global to the process.
    """

    def __init__(self):
        # Initialize to 0, but we never return 0
        self.global_id = 0

    def next(self):
        # The id starts at 1
        self.global_id += 1
        return self.global_id


global_id_manager = GlobalCounter()


class LoraModelLoader:
    """Download Lora weights from remote, and manage a CPU memory cache.

    This entire downloader is sync.

    Args:
        lora_root: Path to directory where LoRA weights will be cached.
        download_timeout_s: How much time the download subprocess has to download
            a single LoRA before a timeout. None means no timeout.
        max_tries: Number of times to try downloading a LoRA model if
            the download subprocess fails.
    """

    def __init__(
        self,
        lora_root: Optional[str] = None,
        download_timeout_s: Optional[float] = None,
        max_tries: int = 1,
    ):
        self.lora_root = lora_root or "/tmp/ray/llm/lora/cache"
        self.disk_cache: Dict[str, DiskMultiplexConfig] = {}
        self.active_syncing_tasks: Dict[str, asyncio.Task[DiskMultiplexConfig]] = {}
        if download_timeout_s is not None and download_timeout_s <= 0:
            raise ValueError(
                f"download_timeout_s must be None or >0, got {download_timeout_s}"
            )
        self.download_timeout_s = download_timeout_s
        if max_tries < 1:
            raise ValueError(f"max_tries must be >=1, got {max_tries}")
        self.max_tries = max_tries

    async def load_model(
        self, lora_model_id: str, llm_config: LLMConfig
    ) -> DiskMultiplexConfig:
        """Load a model.

        This function will load a Lora model from s3 and cache it on disk and in memory.
        This function runs in a separate thread because it does synchronous disk operations.
        """
        if lora_model_id in self.disk_cache:
            return self.disk_cache[lora_model_id]

        if lora_model_id not in self.active_syncing_tasks:
            lora_mirror_config = await get_lora_mirror_config(lora_model_id, llm_config)
            # Cannot use _load_model directly in create_task
            # due to TypeError: a coroutine was expected, got <Future...
            task = asyncio.create_task(self._load_model_async(lora_mirror_config))
            task.add_done_callback(
                lambda result: self.active_syncing_tasks.pop(lora_model_id, None)
            )
            self.active_syncing_tasks[lora_model_id] = task
        else:
            task = self.active_syncing_tasks[lora_model_id]

        # Ensure that cancellation of the current request doesn't
        # affect other requests
        disk_config = await asyncio.shield(task)

        # If we are successful, add the result to the disk cache
        # This will not be reached if the task raises an exception
        self.disk_cache[lora_model_id] = disk_config

        return disk_config

    async def _load_model_async(
        self, lora_mirror_config: LoraMirrorConfig
    ) -> DiskMultiplexConfig:
        return await self._load_model(lora_mirror_config)

    @make_async
    def _load_model(self, lora_mirror_config: LoraMirrorConfig) -> DiskMultiplexConfig:
        return self._load_model_sync(lora_mirror_config)

    @make_async
    def clear_cache(self):
        """Clear the disk cache

        Note: clear_disk_cache currently blindly clears the disk cache and is not
         thread / process safe because another process
         may be reading the cache as it is being cleared.

         TODO(tchordia): come up with a way to clear the Lora Disk cache.
        """
        clear_directory(self.lora_root)

    def _model_dir_path(self, model_id: str) -> str:
        """Construct the path for the lora weight.

        Given a lora model id is expected to be in the format of
            base_model_id:lora_id
        This function will return the path to the directory where the lora weights
            lora_root/lora_id
        """
        lora_id = get_lora_id(clean_model_id(model_id))
        path = os.path.join(self.lora_root, lora_id)
        os.makedirs(path, exist_ok=True)
        return path

    def _download_lora(self, lora_mirror_config: LoraMirrorConfig) -> str:
        # Note (genesu): `model_local_path` affects where the lora weights are stored
        # on local disk.
        model_local_path = self._model_dir_path(lora_mirror_config.lora_model_id)
        sync_model(
            lora_mirror_config.bucket_uri,
            model_local_path,
            timeout=self.download_timeout_s,
            sync_args=lora_mirror_config.sync_args,
        )
        return model_local_path

    def _load_model_sync(
        self, lora_mirror_config: LoraMirrorConfig
    ) -> DiskMultiplexConfig:
        """Load a model from the given mirror configuration."""
        # Apply retry decorator to _download_lora at runtime with instance parameters
        download_with_retries = retry_with_exponential_backoff(
            max_tries=self.max_tries,
            exception_to_check=Exception,  # Catch any exception from CloudFileSystem
        )(lambda config: self._download_lora(config))

        local_path = download_with_retries(lora_mirror_config)
        # the lora_assigned_id is consistent for the lifetime of the disk cache entry
        # If the disk cache is cleared, a new id will be generated.
        return DiskMultiplexConfig.model_validate(
            {
                "model_id": lora_mirror_config.lora_model_id,
                "max_total_tokens": lora_mirror_config.max_total_tokens,
                "local_path": local_path,
                "lora_assigned_int_id": global_id_manager.next(),
            }
        )
