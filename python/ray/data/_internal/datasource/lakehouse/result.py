"""Per-task write result produced by every lakehouse adapter.

A single Ray Data write task collects one of these from the framework and the
driver aggregates them in ``LakehouseDatasink.on_write_complete``.

``FileAction`` is the adapter-defined per-file metadata type (e.g. PyIceberg's
``DataFile`` or deltalake's ``AddAction``). The framework treats it as opaque.
"""

from dataclasses import dataclass, field
from typing import Generic, List, Optional, TypeVar

import pyarrow as pa

FileAction = TypeVar("FileAction")


@dataclass
class LakehouseWriteTaskResult(Generic[FileAction]):
    """Result a worker returns to the driver after a write task completes.

    Attributes:
        file_actions: One or more per-file metadata objects produced by the
            adapter. Driver hands these back to ``adapter.commit``.
        emitted_schemas: PyArrow schema of every non-empty Arrow table the
            worker processed in this task. Driver uses these for type-promoted
            schema unification across workers.
        upsert_keys: Concatenated key-column table for UPSERT mode, or ``None``.
        written_paths: Best-effort list of relative paths written by this task,
            used by ``on_write_failed`` to clean up orphans.
        task_id: Worker task index (for logging / debugging).
    """

    file_actions: List[FileAction] = field(default_factory=list)
    emitted_schemas: List[pa.Schema] = field(default_factory=list)
    upsert_keys: Optional[pa.Table] = None
    written_paths: List[str] = field(default_factory=list)
    task_id: Optional[int] = None
