"""The Iceberg per-file state that travels from listing to reading.

``FileManifest`` is the only payload the framework moves between ``ListFiles``
and ``ReadFiles``, and its three built-in columns (path, size, chunk metadata)
describe a plain file. An Iceberg read also needs each data file's delete
files, partition values and residual filter, which only the listing task (the
one that called ``plan_files()``) has seen.

So each manifest row also carries the planned PyIceberg ``FileScanTask``
itself, pickled with Ray's ``cloudpickle`` as V1's read tasks are, and the
reader hands the unpickled task to ``ArrowScan`` unchanged. Carrying the task
rather than rebuilding it from typed columns keeps this module off PyIceberg's
internal constructors (``DataFile``, ``Record``), which change between
releases. Listing and reading run in one cluster, so both sides use the same
PyIceberg version.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator, Sequence

import pyarrow as pa

from ray import cloudpickle
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)

if TYPE_CHECKING:
    from pyiceberg.table import FileScanTask

# The planned ``FileScanTask``, pickled.
SCAN_TASK_COLUMN_NAME = "__ib_scan_task"


def manifest_from_scan_tasks(tasks: Sequence["FileScanTask"]) -> FileManifest:
    """Encode PyIceberg scan tasks as one manifest block, one row per task."""
    block = pa.table(
        {
            PATH_COLUMN_NAME: pa.array(
                [task.file.file_path for task in tasks], type=pa.string()
            ),
            FILE_SIZE_COLUMN_NAME: pa.array(
                [task.file.file_size_in_bytes for task in tasks], type=pa.int64()
            ),
            FILE_CHUNK_METADATA_COLUMN_NAME: pa.nulls(len(tasks)),
            SCAN_TASK_COLUMN_NAME: pa.array(
                [cloudpickle.dumps(task) for task in tasks], type=pa.binary()
            ),
        }
    )
    return FileManifest(block)


def scan_tasks_from_manifest(manifest: FileManifest) -> Iterator["FileScanTask"]:
    """Unpickle each manifest row's scan task, in row order."""
    for data in manifest.as_block()[SCAN_TASK_COLUMN_NAME].to_pylist():
        yield cloudpickle.loads(data)
