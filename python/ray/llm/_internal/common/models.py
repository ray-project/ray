from typing import Optional

from ray.llm._internal.common.base_pydantic import BaseModelExtended


class DiskMultiplexConfig(BaseModelExtended):
    """Configuration for disk-based model multiplexing.

    This is a shared data structure used by both serve and batch components
    for managing LoRA model loading and caching.
    """

    model_id: str
    max_total_tokens: Optional[int]
    local_path: str

    # this is a per process id assigned to the model
    lora_assigned_int_id: int
