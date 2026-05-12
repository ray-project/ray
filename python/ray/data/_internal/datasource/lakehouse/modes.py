"""Lakehouse-specific enums layered on top of the generic SaveMode.

`SaveMode` is the generic, public-facing enum from ``ray.data._internal.savemode``.
The lakehouse layer re-exports it for convenience and adds ``UpsertSemantics``,
which a ``LakehouseAdapter`` uses to declare whether it supports copy-on-write or
merge-on-read upserts.
"""

from enum import Enum

from ray.data._internal.savemode import SaveMode

__all__ = ["SaveMode", "UpsertSemantics"]


class UpsertSemantics(str, Enum):
    """How a lakehouse adapter implements UPSERT.

    COPY_ON_WRITE
        Matched rows are physically deleted and the new data is appended.
        Used by Delta Lake today and by Iceberg's default upsert path.

    MERGE_ON_READ
        Matched rows are logically masked via delete-files / delete-vectors;
        no physical rewrite. Future Iceberg/Hudi support.
    """

    COPY_ON_WRITE = "copy_on_write"
    MERGE_ON_READ = "merge_on_read"
