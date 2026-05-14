"""Per-task write result produced by every table adapter.

A single Ray Data write task collects one of these from the framework and the
driver aggregates them in ``TableDatasink.on_write_complete``.

``FileAction`` is the adapter-defined per-file metadata type (e.g. PyIceberg's
``DataFile`` or deltalake's ``AddAction``). The framework treats it as opaque,
with one exception: to support duplicate-file detection and orphan cleanup it
needs each action's path, which it obtains via ``TableAdapter.path_for_action``
(not by introspecting a fixed attribute). Adapters whose metadata names the
path field differently override that method — e.g. Iceberg's ``DataFile`` uses
``file_path``.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Generic, List, Optional, TypeVar

import pyarrow as pa

FileAction = TypeVar("FileAction")


@dataclass
class TableWriteTaskResult(Generic[FileAction]):
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
        task_metadata: Adapter-defined free-form metadata produced by this
            task. The framework concatenates these across tasks and forwards
            the list to ``adapter.gather_task_metadata`` before ``commit``,
            giving adapters a channel for worker→driver state such as
            Delta's per-write app-transaction UUID.
    """

    file_actions: List[FileAction] = field(default_factory=list)
    emitted_schemas: List[pa.Schema] = field(default_factory=list)
    upsert_keys: Optional[pa.Table] = None
    written_paths: List[str] = field(default_factory=list)
    task_id: Optional[int] = None
    task_metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def data_files(self) -> List[FileAction]:
        """Deprecated alias for ``file_actions``.

        Provided so code written against the pre-abstraction
        ``IcebergWriteResult.data_files`` keeps working without modification.
        Prefer ``file_actions`` going forward; this alias may be removed in
        a future release.
        """
        return self.file_actions
