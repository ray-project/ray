"""
Configuration classes and enums for Delta Lake datasource.
"""

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional


class WriteMode(Enum):
    """Write modes for Delta Lake tables."""

    ERROR = "error"
    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"


class DeltaJSONEncoder(json.JSONEncoder):
    """JSON encoder for Delta Lake data types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, bytes):
            return obj.decode("unicode_escape", "backslashreplace")
        elif isinstance(obj, (date, datetime)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


@dataclass
class DeltaWriteConfig:
    """Configuration for Delta Lake write operations."""

    mode: WriteMode = WriteMode.APPEND
    partition_cols: Optional[List[str]] = None
    schema_mode: str = "merge"
    name: Optional[str] = None
    description: Optional[str] = None
    configuration: Optional[Dict[str, str]] = None
    custom_metadata: Optional[Dict[str, str]] = None
    target_file_size: Optional[int] = None
    writer_properties: Optional[Any] = None
    post_commithook_properties: Optional[Any] = None
    commit_properties: Optional[Any] = None
    storage_options: Optional[Dict[str, str]] = None
    engine: str = "rust"
    overwrite_schema: bool = False
    schema: Optional[Any] = None
