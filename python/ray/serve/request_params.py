from dataclasses import dataclass
from typing import Optional

from ray.serve.context import TaskContext


@dataclass
class RequestMetadata:
    endpoint: str
    request_context: TaskContext

    call_method: str = "__call__"
    shard_key: Optional[str] = None
    http_method: str = "GET"
    is_shadow_query: bool = False
