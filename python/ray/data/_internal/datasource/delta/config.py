"""Configuration and result types for Delta Lake datasource."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    import pyarrow as pa
    from deltalake.transaction import AddAction


@dataclass
class DeltaWriteResult:
    """Result from writing blocks to Delta Lake storage.

    Attributes:
        add_actions: File metadata for Delta transaction log.
        upsert_keys: Key columns for upsert operations.
        schemas: Schemas from written blocks.
    """

    add_actions: List["AddAction"] = field(default_factory=list)
    upsert_keys: Optional["pa.Table"] = None
    schemas: List["pa.Schema"] = field(default_factory=list)


UPSERT_JOIN_COLS = "join_cols"
