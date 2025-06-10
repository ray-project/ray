from typing import Optional

from ray.llm._internal.common.utils.download_utils import LoraModelLoader
from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.multiplex.utils import (
    get_lora_mirror_config,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


# Re-export the LoraModelLoader from download_utils for backward compatibility
# This allows existing imports to continue working
__all__ = ["LoraModelLoader"]


# Backward compatibility function - delegates to the unified implementation
async def load_lora_model(
    lora_model_id: str,
    llm_config: LLMConfig,
    lora_root: Optional[str] = None,
    download_timeout_s: Optional[float] = None,
    max_tries: int = 1,
) -> DiskMultiplexConfig:
    """Load a LoRA model using the unified downloading functionality.

    This is a convenience function that creates a LoraModelLoader instance
    and uses it to load the model.

    Args:
        lora_model_id: The LoRA model ID to load.
        llm_config: The LLM configuration containing LoRA settings.
        lora_root: Path to directory where LoRA weights will be cached.
        download_timeout_s: Download timeout in seconds.
        max_tries: Number of retry attempts.

    Returns:
        A DiskMultiplexConfig containing the loaded model information.
    """
    loader = LoraModelLoader(
        lora_root=lora_root,
        download_timeout_s=download_timeout_s,
        max_tries=max_tries,
    )

    lora_mirror_config = await get_lora_mirror_config(lora_model_id, llm_config)
    return await loader.load_model(lora_model_id, lora_mirror_config)
