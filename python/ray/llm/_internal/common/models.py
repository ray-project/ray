"""Common data models and utilities shared across Ray LLM modules."""

import threading
from typing import Optional

from pydantic import BaseModel


class DiskMultiplexConfig(BaseModel):
    """Configuration for disk-based model multiplexing."""

    model_id: str
    max_total_tokens: Optional[int] = None
    local_path: str
    lora_assigned_int_id: int


class GlobalIdManager:
    """Thread-safe global ID manager for assigning unique IDs."""

    def __init__(self):
        self._counter = 0
        self._lock = threading.Lock()

    def next(self) -> int:
        """Get the next unique ID."""
        with self._lock:
            self._counter += 1
            return self._counter


# Global instance
global_id_manager = GlobalIdManager()
